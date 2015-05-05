package org.ehcache.decorators;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import net.sf.ehcache.CacheException;
import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Element;
import net.sf.ehcache.bootstrap.BootstrapCacheLoader;
import net.sf.ehcache.loader.CacheLoader;
import net.sf.ehcache.search.Attribute;
import net.sf.ehcache.search.Query;
import net.sf.ehcache.search.Result;
import net.sf.ehcache.search.Results;
import net.sf.ehcache.search.expression.Criteria;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CachePartitionDecorator extends EhcacheDelegatedDecoratorAdapter {
	private static final Logger log = LoggerFactory.getLogger(CachePartitionDecorator.class);

	private static final String PROPNAME_SYNCPOOLSIZE = "syncPoolSize";
	private static final String PROPNAME_REFRESHINTERVAL = "refreshIntervalInMilliSeconds";
	private static final String PROPNAME_PARTITIONID = "partitionId";
	private static final int POOLSIZEDEFAULT = 10;
	private static final long REFRESHINTERVALDEFAULT = 5000L;
	
	private final int partitionId;
	private final ScheduledExecutorService cacheTimerService;
	private final ExecutorService cacheSyncService;

	//instanciated at init() time
	private volatile boolean initialized = false;
	private long refreshInterval = REFRESHINTERVALDEFAULT;
	private static final TimeUnit refreshIntervalUnit = TimeUnit.MILLISECONDS;
	
	
	public CachePartitionDecorator(Ehcache underlyingCache, Properties properties) {
		super(underlyingCache, properties);
		
		this.partitionId = Integer.parseInt(properties.getProperty(PROPNAME_PARTITIONID, "-1"));
		if(partitionId == -1)
			throw new CacheException("Decorator on " + underlyingCache.getName() + " should be configured with a value for property " + PROPNAME_PARTITIONID);
		
		int syncPoolSize;
		try {
			syncPoolSize = Integer.parseInt(properties.getProperty(PROPNAME_SYNCPOOLSIZE));
			if(syncPoolSize < 1){
				log.warn(String.format("Poolsize cannot be 0 or less...reverting to default: %d", POOLSIZEDEFAULT));
				syncPoolSize = POOLSIZEDEFAULT;
			}
		} catch (NumberFormatException e) {
			log.warn(String.format("Poolsize value is not specified or not valid...reverting to default: %d", POOLSIZEDEFAULT));
			syncPoolSize = POOLSIZEDEFAULT;
		}

		try {
			refreshInterval = Long.parseLong(properties.getProperty(PROPNAME_REFRESHINTERVAL));
			if(refreshInterval < 1){
				log.warn(String.format("Refresh interval cannot be 0 or less...reverting to default: %d", REFRESHINTERVALDEFAULT));
				refreshInterval = REFRESHINTERVALDEFAULT;
			}
		} catch (NumberFormatException e) {
			log.warn(String.format("Refresh Interval value is not specified or not valid...reverting to default: %d", REFRESHINTERVALDEFAULT));
			refreshInterval = REFRESHINTERVALDEFAULT;
		}

		cacheSyncService = Executors.newFixedThreadPool(syncPoolSize, new NamedThreadFactory("Sync Cache Pool"));
		
		//setup the timer thread pool for every 5 seconds
		cacheTimerService = Executors.newScheduledThreadPool(1, new NamedThreadFactory("Sync Timer Cache Pool"));
	}

	/*
	 * initialize the refresh service
	 * @see org.terracotta.utils.EhcacheDelegatedDecoratorAdapter#init()
	 */
	protected void init() {
		super.init();
		if(!initialized) {
			synchronized (this) {
				if(!initialized){
					if(delegatedCache == null)
						throw new CacheException("Delegated cache does not exist...");
					
					final Attribute<Integer> partitionSearchAttribute = delegatedCache.getSearchAttribute(PROPNAME_PARTITIONID);
					if(null == partitionSearchAttribute)
						throw new CacheException("Delegated cache should have a searchable attribute named " + PROPNAME_PARTITIONID);
					
					//schedule the timer pool to execute a cache search every 5 seconds...which in turn will execute the cache sync operations
					cacheTimerService.scheduleAtFixedRate(new TimedRefreshSyncOp(partitionSearchAttribute, this.partitionId), 0L, refreshInterval, refreshIntervalUnit);
					
					initialized = true;
				}
			}
		}
	}
	
	/*
	 * Performs get operation: first check in underlying cache, then if not found, in delegated cache.
	 * @see org.terracotta.utils.EhcacheDelegatedDecoratorAdapter#get(java.lang.Object)
	 */
	@Override
	public Element get(Object key) throws IllegalStateException, CacheException {
		init();
		Element e = underlyingCache.get(key);
		if(e == null) {
			e = delegatedCache.get(key);
			if(log.isDebugEnabled()) {
				log.debug("----> get Timestamp " + System.currentTimeMillis() +  " - Faulting entry with key =" + key);
			}
		}
		return e;
	}

	/*
	 * Performs get operation: first check in underlying cache, then if not found, in delegated cache.
	 * @see org.terracotta.utils.EhcacheDelegatedDecoratorAdapter#get(java.io.Serializable)
	 */
	@Override
	public Element get(Serializable key) {
		return this.get((Object)key);
	}

	/*
	 * Performs bulk get using mutli-threaded service on @see org.terracotta.utils.CachePartitionDecorator#get(java.io.Serializable)
	 * @see org.terracotta.utils.EhcacheDelegatedDecoratorAdapter#getAll(java.util.Collection)
	 */
	@Override
	public Map<Object, Element> getAll(Collection<?> keys) throws IllegalStateException, CacheException, NullPointerException {
		init();
		Map<Object, Element> result = new HashMap<Object, Element>();
		Iterator<?> it = keys.iterator();
		Future<Element> futs[] = new Future[keys.size()];
		int count = 0;
		while(it.hasNext()) {
			futs[count++] = cacheSyncService.submit(new GetOp(it.next()));
		}

		for(int i = 0; i < count; i++) {
			Element el = null;
			try {
				el = futs[i].get();
			}catch(Exception e) {
				log.warn("Error while getting the data from cache", e);
			}
			if(el != null && el.getValue() != null) {
				result.put(el.getObjectKey(), el);
			}
		}
		return result;
	}

	/*
	 * Searches elements in delegated cache, and call refreshOp for every returned results
	 */
	private class TimedRefreshSyncOp implements Runnable {
		private Query query;

		public TimedRefreshSyncOp(Attribute<Integer> partitionSearchAttribute, Integer partitionId) {
			this.query = delegatedCache.createQuery();
			Criteria searchCriteria = partitionSearchAttribute.eq(partitionId);
			query.addCriteria(searchCriteria);
			query.includeKeys();
			query.end();
		}

		public void run() {
			List keys = searchPartitionKeys(query);
			Future futs[] = new Future[keys.size()];
			int count = 0;
			for(Object key: keys){
				futs[count++] = cacheSyncService.submit(new RefreshOp(key));
			}
			for(int i = 0; i < count; i++) {
				try {
					futs[i].get();
				}catch(Exception e) {
					log.warn("Error while putting the data into cache", e);
				}
			}
		}
		
		private List searchPartitionKeys(final Query query) {
			LinkedList<Object> keys = new LinkedList<Object>();
			System.out.println("Starting search...");
			long startTime = System.currentTimeMillis();
			Results results = query.execute();
			if(log.isDebugEnabled())
				log.debug(String.format("Search time: %d ms", System.currentTimeMillis() - startTime));

			// perform the refresh
			for (Result result : results.all()) {
				keys.add(result.getKey());
			}

			results.discard();

			return keys;
		}
	}

	/*
	 * Reads from delegated cache, and update decorated cache with value
	 */
	private class RefreshOp implements Runnable {
		private Object keyToUpdate;

		public RefreshOp(Object key) {
			this.keyToUpdate = key;
		}

		public void run() {
			try {
				final Element replacementElement = delegatedCache.getQuiet(keyToUpdate);
				if (replacementElement == null) {
					if (log.isDebugEnabled()) {
						log.debug(delegatedCache.getName() + ": entry with key " + keyToUpdate + " has been removed - skipping it");
					}
					underlyingCache.remove(keyToUpdate);
				} else {
					underlyingCache.put(replacementElement);
				}
			} catch (final Exception e) {
				// Collect the exception and keep going.
				log.warn(getName() + "Could not refresh element " + keyToUpdate, e);
			}
		}
	}

	/*
	 * Performs get operation: first check in underlying cache, then if not found, in delegated cache.
	 */
	private class GetOp implements Callable<Element> {
		private Object key;
		public GetOp(Object key) {
			this.key = key;
		}

		public Element call() {
			return CachePartitionDecorator.this.get(key);
		}
	}

	/*
	 * shutdown hook: shutting down all the executors used in this class
	 */
	public void shutdown() throws InterruptedException{
		log.info("Shutting down Cache Service");
		shutdownAndAwaitTermination(cacheTimerService);
		shutdownAndAwaitTermination(cacheSyncService);
	}

	/*
	 * thread executor shutdown
	 */
	private void shutdownAndAwaitTermination(ExecutorService pool) {
		pool.shutdown(); // Disable new tasks from being submitted

		try {
			// Wait until existing tasks to terminate
			while(!pool.awaitTermination(5, TimeUnit.SECONDS));

			pool.shutdownNow(); // Cancel currently executing tasks

			// Wait a while for tasks to respond to being canceled
			if (!pool.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS))
				log.error("Pool did not terminate");
		} catch (InterruptedException ie) {
			// (Re-)Cancel if current thread also interrupted
			pool.shutdownNow();

			// Preserve interrupt status
			Thread.currentThread().interrupt();
		}
	}
	
	@Override
	public final void load(Object key) throws CacheException {
		throw new UnsupportedOperationException("Cache is read-only...operation not supported");
	}

	@Override
	public final void loadAll(Collection keys, Object argument) throws CacheException {
		throw new UnsupportedOperationException("Cache is read-only...operation not supported");
	}

	@Override
	public final void put(Element element, boolean doNotNotifyCacheReplicators)
			throws IllegalArgumentException, IllegalStateException,
			CacheException {
		throw new UnsupportedOperationException("Cache is read-only...operation not supported");
	}

	@Override
	public final void put(Element element) throws IllegalArgumentException,
			IllegalStateException, CacheException {
		throw new UnsupportedOperationException("Cache is read-only...operation not supported");
	}

	@Override
	public final void putAll(Collection<Element> elements)
			throws IllegalArgumentException, IllegalStateException,
			CacheException {
		throw new UnsupportedOperationException("Cache is read-only...operation not supported");
	}

	@Override
	public final Element putIfAbsent(Element element,
			boolean doNotNotifyCacheReplicators) throws NullPointerException {
		throw new UnsupportedOperationException("Cache is read-only...operation not supported");
	}

	@Override
	public final Element putIfAbsent(Element element) throws NullPointerException {
		throw new UnsupportedOperationException("Cache is read-only...operation not supported");
	}

	@Override
	public final void putQuiet(Element element) throws IllegalArgumentException,
			IllegalStateException, CacheException {
		throw new UnsupportedOperationException("Cache is read-only...operation not supported");
	}

	@Override
	public final void putWithWriter(Element element) throws IllegalArgumentException,
			IllegalStateException, CacheException {
		throw new UnsupportedOperationException("Cache is read-only...operation not supported");
	}

	@Override
	public final void registerCacheLoader(CacheLoader cacheLoader) {
		throw new UnsupportedOperationException("Cache is read-only...operation not supported");
	}

	@Override
	public final boolean replace(Element old, Element element)
			throws NullPointerException, IllegalArgumentException {
		throw new UnsupportedOperationException("Cache is read-only...operation not supported");
	}

	@Override
	public final Element replace(Element element) throws NullPointerException {
		throw new UnsupportedOperationException("Cache is read-only...operation not supported");
	}

	@Override
	public final void setBootstrapCacheLoader(
			BootstrapCacheLoader bootstrapCacheLoader) throws CacheException {
		throw new UnsupportedOperationException("Cache is read-only...operation not supported");
	}
}
