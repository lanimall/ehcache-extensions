package org.ehcache.decorators;

import java.beans.PropertyChangeListener;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheException;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Element;
import net.sf.ehcache.Status;
import net.sf.ehcache.bootstrap.BootstrapCacheLoader;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.constructs.EhcacheDecoratorAdapter;
import net.sf.ehcache.event.RegisteredEventListeners;
import net.sf.ehcache.exceptionhandler.CacheExceptionHandler;
import net.sf.ehcache.extension.CacheExtension;
import net.sf.ehcache.loader.CacheLoader;
import net.sf.ehcache.search.Attribute;
import net.sf.ehcache.search.Query;
import net.sf.ehcache.search.attribute.DynamicAttributesExtractor;
import net.sf.ehcache.statistics.StatisticsGateway;
import net.sf.ehcache.terracotta.TerracottaNotRunningException;
import net.sf.ehcache.transaction.manager.TransactionManagerLookup;
import net.sf.ehcache.writer.CacheWriter;
import net.sf.ehcache.writer.CacheWriterManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * A decorator adapter that performs all the same operations as identified in EhcacheDecoratorAdapter,
 * But make sure to call init() hook once, from any method call.
 */
public class EhcacheDelegatedDecoratorAdapter extends EhcacheDecoratorAdapter {
	private static final Logger log = LoggerFactory.getLogger(EhcacheDelegatedDecoratorAdapter.class);
	
	private static final String PROPNAME_DELEGATECACHENAME = "delegateCacheName";
	
	private final String delegatedCacheName;
	
	//these are accessed/populated at init() time on purpose...as delegatedCache is not available at constructor time
	private volatile boolean initialized = false;
	protected Cache delegatedCache = null;

	public EhcacheDelegatedDecoratorAdapter(Ehcache underlyingCache, Properties properties) {
		super(underlyingCache);
		
		this.delegatedCacheName = properties.getProperty(PROPNAME_DELEGATECACHENAME);
		if(delegatedCacheName == null || "".equals(delegatedCacheName))
			throw new CacheException("Decorator on " + underlyingCache.getName() + " should be configured with a terracotta cache specified by property " + PROPNAME_DELEGATECACHENAME);
	}

	//init should be called once only when any method in that cache is called
	protected void init() {
		if(!initialized) {
			synchronized (this) {
				if(!initialized){
					delegatedCache = underlyingCache.getCacheManager().getCache(delegatedCacheName);
					if(delegatedCache == null)
						throw new CacheException("Cache " + delegatedCacheName + " configured through cache decorator property '" + PROPNAME_DELEGATECACHENAME + "' doesn't exist");
					initialized = true;
				}
			}
		}
	}

	/*
	 * no init on getName, as getName is called too soon by the framework...
	 * @see net.sf.ehcache.constructs.EhcacheDecoratorAdapter#getName()
	 */
	@Override
	public String getName() {
		return super.getName();
	}
	
	@Override
	public String getGuid() {
		return super.getGuid();
	}
	
	@Override
	public void acquireReadLockOnKey(Object key) {
		init();
		super.acquireReadLockOnKey(key);
	}

	@Override
	public void acquireWriteLockOnKey(Object key) {
		init();
		super.acquireWriteLockOnKey(key);
	}

	@Override
	public void addPropertyChangeListener(PropertyChangeListener listener) {
		init();
		super.addPropertyChangeListener(listener);
	}

	@Override
	public void bootstrap() {
		init();
		super.bootstrap();
	}

	@Override
	@Deprecated
	public long calculateInMemorySize() throws IllegalStateException,
	CacheException {
		init();
		return super.calculateInMemorySize();
	}

	@Override
	@Deprecated
	public long calculateOffHeapSize() throws IllegalStateException,
	CacheException {
		init();
		return super.calculateOffHeapSize();
	}

	@Override
	@Deprecated
	public long calculateOnDiskSize() throws IllegalStateException,
	CacheException {
		init();
		return super.calculateOnDiskSize();
	}

	@Override
	public Object clone() throws CloneNotSupportedException {
		init();
		return super.clone();
	}

	@Override
	public Query createQuery() {
		init();
		return super.createQuery();
	}

	@Override
	public void disableDynamicFeatures() {
		init();
		super.disableDynamicFeatures();
	}

	@Override
	public void dispose() throws IllegalStateException {
		init();
		super.dispose();
	}

	@Override
	public void evictExpiredElements() {
		init();
		super.evictExpiredElements();
	}

	@Override
	public void flush() throws IllegalStateException, CacheException {
		init();
		super.flush();
	}

	@Override
	public Element get(Object key) throws IllegalStateException, CacheException {
		init();
		return super.get(key);
	}

	@Override
	public Element get(Serializable key) throws IllegalStateException,
	CacheException {
		init();
		return super.get(key);
	}

	@Override
	public Map<Object, Element> getAll(Collection<?> keys)
			throws IllegalStateException, CacheException {
		init();
		return super.getAll(keys);
	}

	@Override
	public Map getAllWithLoader(Collection keys, Object loaderArgument)
			throws CacheException {
		init();
		return super.getAllWithLoader(keys, loaderArgument);
	}

	@Override
	public BootstrapCacheLoader getBootstrapCacheLoader() {
		init();
		return super.getBootstrapCacheLoader();
	}

	@Override
	public CacheConfiguration getCacheConfiguration() {
		init();
		return super.getCacheConfiguration();
	}

	@Override
	public RegisteredEventListeners getCacheEventNotificationService() {
		init();
		return super.getCacheEventNotificationService();
	}

	@Override
	public CacheExceptionHandler getCacheExceptionHandler() {
		init();
		return super.getCacheExceptionHandler();
	}

	@Override
	public CacheManager getCacheManager() {
		init();
		return super.getCacheManager();
	}

	@Override
	@Deprecated
	public int getDiskStoreSize() throws IllegalStateException {
		init();
		return super.getDiskStoreSize();
	}

	@Override
	public Object getInternalContext() {
		init();
		return super.getInternalContext();
	}

	@Override
	public List getKeys() throws IllegalStateException, CacheException {
		init();
		return super.getKeys();
	}

	@Override
	public List getKeysNoDuplicateCheck() throws IllegalStateException {
		init();
		return super.getKeysNoDuplicateCheck();
	}

	@Override
	public List getKeysWithExpiryCheck() throws IllegalStateException,
	CacheException {
		init();
		return super.getKeysWithExpiryCheck();
	}

	@Override
	@Deprecated
	public long getMemoryStoreSize() throws IllegalStateException {
		init();
		return super.getMemoryStoreSize();
	}

	@Override
	@Deprecated
	public long getOffHeapStoreSize() throws IllegalStateException {
		init();
		return super.getOffHeapStoreSize();
	}

	@Override
	public Element getQuiet(Object key) throws IllegalStateException,
	CacheException {
		init();
		return super.getQuiet(key);
	}

	@Override
	public Element getQuiet(Serializable key) throws IllegalStateException,
	CacheException {
		init();
		return super.getQuiet(key);
	}

	@Override
	public List<CacheExtension> getRegisteredCacheExtensions() {
		init();
		return super.getRegisteredCacheExtensions();
	}

	@Override
	public List<CacheLoader> getRegisteredCacheLoaders() {
		init();
		return super.getRegisteredCacheLoaders();
	}

	@Override
	public CacheWriter getRegisteredCacheWriter() {
		init();
		return super.getRegisteredCacheWriter();
	}

	@Override
	public <T> Attribute<T> getSearchAttribute(String attributeName)
			throws CacheException {
		init();
		return super.getSearchAttribute(attributeName);
	}

	@Override
	public int getSize() throws IllegalStateException, CacheException {
		init();
		return super.getSize();
	}

	@Override
	public StatisticsGateway getStatistics() throws IllegalStateException {
		init();
		return super.getStatistics();
	}

	@Override
	public Status getStatus() {
		init();
		return super.getStatus();
	}

	@Override
	public Element getWithLoader(Object key, CacheLoader loader,
			Object loaderArgument) throws CacheException {
		init();
		return super.getWithLoader(key, loader, loaderArgument);
	}

	@Override
	public CacheWriterManager getWriterManager() {
		init();
		return super.getWriterManager();
	}

	@Override
	public boolean hasAbortedSizeOf() {
		init();
		return super.hasAbortedSizeOf();
	}

	@Override
	public void initialise() {
		init();
		super.initialise();
	}

	@Override
	public boolean isClusterBulkLoadEnabled()
			throws UnsupportedOperationException, TerracottaNotRunningException {
		init();
		return super.isClusterBulkLoadEnabled();
	}

	@Override
	@Deprecated
	public boolean isClusterCoherent() {
		init();
		return super.isClusterCoherent();
	}

	@Override
	public boolean isDisabled() {
		init();
		return super.isDisabled();
	}

	@Override
	public boolean isElementInMemory(Object key) {
		init();
		return super.isElementInMemory(key);
	}

	@Override
	public boolean isElementInMemory(Serializable key) {
		init();
		return super.isElementInMemory(key);
	}

	@Override
	public boolean isElementOnDisk(Object key) {
		init();
		return super.isElementOnDisk(key);
	}

	@Override
	public boolean isElementOnDisk(Serializable key) {
		init();
		return super.isElementOnDisk(key);
	}

	@Override
	public boolean isExpired(Element element) throws IllegalStateException,
	NullPointerException {
		init();
		return super.isExpired(element);
	}

	@Override
	public boolean isKeyInCache(Object key) {
		init();
		return super.isKeyInCache(key);
	}

	@Override
	public boolean isNodeBulkLoadEnabled()
			throws UnsupportedOperationException, TerracottaNotRunningException {
		init();
		return super.isNodeBulkLoadEnabled();
	}

	@Override
	@Deprecated
	public boolean isNodeCoherent() {
		init();
		return super.isNodeCoherent();
	}

	@Override
	public boolean isReadLockedByCurrentThread(Object key) {
		init();
		return super.isReadLockedByCurrentThread(key);
	}

	@Override
	public boolean isSearchable() {
		init();
		return super.isSearchable();
	}

	@Override
	public boolean isValueInCache(Object value) {
		init();
		return super.isValueInCache(value);
	}

	@Override
	public boolean isWriteLockedByCurrentThread(Object key) {
		init();
		return super.isWriteLockedByCurrentThread(key);
	}

	@Override
	public void load(Object key) throws CacheException {
		init();
		super.load(key);
	}

	@Override
	public void loadAll(Collection keys, Object argument) throws CacheException {
		init();
		super.loadAll(keys, argument);
	}

	@Override
	public void put(Element element, boolean doNotNotifyCacheReplicators)
			throws IllegalArgumentException, IllegalStateException,
			CacheException {
		init();
		super.put(element, doNotNotifyCacheReplicators);
	}

	@Override
	public void put(Element element) throws IllegalArgumentException,
	IllegalStateException, CacheException {
		init();
		super.put(element);
	}

	@Override
	public void putAll(Collection<Element> elements)
			throws IllegalArgumentException, IllegalStateException,
			CacheException {
		init();
		super.putAll(elements);
	}

	@Override
	public Element putIfAbsent(Element element,
			boolean doNotNotifyCacheReplicators) throws NullPointerException {
		init();
		return super.putIfAbsent(element, doNotNotifyCacheReplicators);
	}

	@Override
	public Element putIfAbsent(Element element) throws NullPointerException {
		init();
		return super.putIfAbsent(element);
	}

	@Override
	public void putQuiet(Element element) throws IllegalArgumentException,
	IllegalStateException, CacheException {
		init();
		super.putQuiet(element);
	}

	@Override
	public void putWithWriter(Element element) throws IllegalArgumentException,
	IllegalStateException, CacheException {
		init();
		super.putWithWriter(element);
	}

	@Override
	public void recalculateSize(Object key) {
		init();
		super.recalculateSize(key);
	}

	@Override
	public void registerCacheExtension(CacheExtension cacheExtension) {
		init();
		super.registerCacheExtension(cacheExtension);
	}

	@Override
	public void registerCacheLoader(CacheLoader cacheLoader) {
		init();
		super.registerCacheLoader(cacheLoader);
	}

	@Override
	public void registerCacheWriter(CacheWriter cacheWriter) {
		init();
		super.registerCacheWriter(cacheWriter);
	}

	@Override
	public void registerDynamicAttributesExtractor(
			DynamicAttributesExtractor extractor) {
		init();
		super.registerDynamicAttributesExtractor(extractor);
	}

	@Override
	public void releaseReadLockOnKey(Object key) {
		init();
		super.releaseReadLockOnKey(key);
	}

	@Override
	public void releaseWriteLockOnKey(Object key) {
		init();
		super.releaseWriteLockOnKey(key);
	}

	@Override
	public boolean remove(Object key, boolean doNotNotifyCacheReplicators)
			throws IllegalStateException {
		init();
		return super.remove(key, doNotNotifyCacheReplicators);
	}

	@Override
	public boolean remove(Object key) throws IllegalStateException {
		init();
		return super.remove(key);
	}

	@Override
	public boolean remove(Serializable key, boolean doNotNotifyCacheReplicators)
			throws IllegalStateException {
		init();
		return super.remove(key, doNotNotifyCacheReplicators);
	}

	@Override
	public boolean remove(Serializable key) throws IllegalStateException {
		init();
		return super.remove(key);
	}

	@Override
	public void removeAll() throws IllegalStateException, CacheException {
		init();
		super.removeAll();
	}

	@Override
	public void removeAll(boolean doNotNotifyCacheReplicators)
			throws IllegalStateException, CacheException {
		init();
		super.removeAll(doNotNotifyCacheReplicators);
	}

	@Override
	public void removeAll(Collection<?> keys,
			boolean doNotNotifyCacheReplicators) throws IllegalStateException {
		init();
		super.removeAll(keys, doNotNotifyCacheReplicators);
	}

	@Override
	public void removeAll(Collection<?> keys) throws IllegalStateException {
		init();
		super.removeAll(keys);
	}

	@Override
	public Element removeAndReturnElement(Object key)
			throws IllegalStateException {
		init();
		return super.removeAndReturnElement(key);
	}

	@Override
	public boolean removeElement(Element element) throws NullPointerException {
		init();
		return super.removeElement(element);
	}

	@Override
	public void removePropertyChangeListener(PropertyChangeListener listener) {
		init();
		super.removePropertyChangeListener(listener);
	}

	@Override
	public boolean removeQuiet(Object key) throws IllegalStateException {
		init();
		return super.removeQuiet(key);
	}

	@Override
	public boolean removeQuiet(Serializable key) throws IllegalStateException {
		init();
		return super.removeQuiet(key);
	}

	@Override
	public boolean removeWithWriter(Object key) throws IllegalStateException,
	CacheException {
		init();
		return super.removeWithWriter(key);
	}

	@Override
	public boolean replace(Element old, Element element)
			throws NullPointerException, IllegalArgumentException {
		init();
		return super.replace(old, element);
	}

	@Override
	public Element replace(Element element) throws NullPointerException {
		init();
		return super.replace(element);
	}

	@Override
	public void setBootstrapCacheLoader(
			BootstrapCacheLoader bootstrapCacheLoader) throws CacheException {
		init();
		super.setBootstrapCacheLoader(bootstrapCacheLoader);
	}

	@Override
	public void setCacheExceptionHandler(
			CacheExceptionHandler cacheExceptionHandler) {
		init();
		super.setCacheExceptionHandler(cacheExceptionHandler);
	}

	@Override
	public void setCacheManager(CacheManager cacheManager) {
		init();
		super.setCacheManager(cacheManager);
	}

	@Override
	public void setDisabled(boolean disabled) {
		init();
		super.setDisabled(disabled);
	}

	@Override
	public void setName(String name) {
		init();
		super.setName(name);
	}

	@Override
	public void setNodeBulkLoadEnabled(boolean enabledBulkLoad)
			throws UnsupportedOperationException, TerracottaNotRunningException {
		init();
		super.setNodeBulkLoadEnabled(enabledBulkLoad);
	}

	@Override
	@Deprecated
	public void setNodeCoherent(boolean coherent)
			throws UnsupportedOperationException {
		init();
		super.setNodeCoherent(coherent);
	}

	@Override
	public void setTransactionManagerLookup(
			TransactionManagerLookup transactionManagerLookup) {
		init();
		super.setTransactionManagerLookup(transactionManagerLookup);
	}

	@Override
	public String toString() {
		init();
		return super.toString();
	}

	@Override
	public boolean tryReadLockOnKey(Object key, long timeout)
			throws InterruptedException {
		init();
		return super.tryReadLockOnKey(key, timeout);
	}

	@Override
	public boolean tryWriteLockOnKey(Object key, long timeout)
			throws InterruptedException {
		init();
		return super.tryWriteLockOnKey(key, timeout);
	}

	@Override
	public void unregisterCacheExtension(CacheExtension cacheExtension) {
		init();
		super.unregisterCacheExtension(cacheExtension);
	}

	@Override
	public void unregisterCacheLoader(CacheLoader cacheLoader) {
		init();
		super.unregisterCacheLoader(cacheLoader);
	}

	@Override
	public void unregisterCacheWriter() {
		init();
		super.unregisterCacheWriter();
	}

	@Override
	public void waitUntilClusterBulkLoadComplete()
			throws UnsupportedOperationException, TerracottaNotRunningException {
		init();
		super.waitUntilClusterBulkLoadComplete();
	}

	@Override
	@Deprecated
	public void waitUntilClusterCoherent() throws UnsupportedOperationException {
		init();
		super.waitUntilClusterCoherent();
	}
}
