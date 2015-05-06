package com.softwareag.terracotta;

import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

/**
 * Created by Fabien Sanglier on 5/4/15.
 */
public class EhcacheOutputStream extends OutputStream {
    private static int DEFAULT_BUFFER_SIZE = 5 * 1024 * 1024; // 5MB

    /**
     * The internal buffer where data is stored.
     */
    protected byte buf[];

    /**
     * The number of valid bytes in the buffer. This value is always
     * in the range <tt>0</tt> through <tt>buf.length</tt>; elements
     * <tt>buf[0]</tt> through <tt>buf[count-1]</tt> contain valid
     * byte data.
     */
    protected int count;

    /*
     * The Internal Ehcache cache object
     */
    protected Cache cache;

    /*
     * The Ehcache cache key object the data should get written to
     */
    protected Object cacheKey;

    /*
     * The number of cache entry chunks
     */
    protected volatile EhcacheStreamMasterIndex currentStreamMasterIndex = null;

    /**
     * Creates a new buffered output stream to write data to a cache
     *
     * @param   cache   the underlying cache to copy data to
     */
    public EhcacheOutputStream(Cache cache, Object cacheKey) {
        this(cache, cacheKey, DEFAULT_BUFFER_SIZE);
    }

    /**
     * Creates a new buffered output stream to write data to a cache
     * with the specified buffer size.
     *
     * @param   cache   the underlying cache to copy data to
     * @param   size   the buffer size.
     * @exception IllegalArgumentException if size &lt;= 0.
     */
    public EhcacheOutputStream(Cache cache, Object cacheKey, int size) {
        if (size <= 0) {
            throw new IllegalArgumentException("Buffer size <= 0");
        }
        this.buf = new byte[size];
        this.cache = cache;
        this.cacheKey = cacheKey;
        this.currentStreamMasterIndex = null;
    }

    public void clearCacheDataForKey() throws IOException {
        EhcacheStreamMasterIndex oldEhcacheStreamMasterIndex = casReplaceEhcacheStreamMasterIndex(null, true);
        if(!clearChunksForKey(oldEhcacheStreamMasterIndex)){
            // could not remove successfully all the chunk entries...
            // but that's not too terrible as long as the EhcacheStreamMasterIndex was removed properly (because the chunks will get overwritten on subsequent writes)
            // do nothing for now...
        }
    }

    private EhcacheStreamKey buildMasterKey(){
        return new EhcacheStreamKey(cacheKey, EhcacheStreamKey.MASTER_INDEX);
    }

    private boolean clearChunksForKey(EhcacheStreamMasterIndex ehcacheStreamMasterIndex) {
        boolean success = false;
        if(null != ehcacheStreamMasterIndex){
            //remove all the chunk entries
            for(int i = 0; i < ehcacheStreamMasterIndex.getNumberOfChunk(); i++){
                cache.remove(new EhcacheStreamKey(cacheKey, i));
            }
            success = true;
        }
        return success;
    }

    private Element getEhcacheStreamMasterIndexElement() {
        return cache.get(buildMasterKey());
    }

    private EhcacheStreamMasterIndex getEhcacheStreamMasterIndexIfWriteable() throws IOException {
        EhcacheStreamMasterIndex cacheMasterIndexForKey = null;
        Element cacheElem;
        if(null != (cacheElem = getEhcacheStreamMasterIndexElement())) {
            cacheMasterIndexForKey = (EhcacheStreamMasterIndex)cacheElem.getObjectValue();
        }

        if(null != cacheMasterIndexForKey && cacheMasterIndexForKey.isCurrentWrite())
            throw new IOException("Concurrent write not allowed - Current cache entry with key[" + cacheKey + "] is currently being written...");

        return cacheMasterIndexForKey;
    }

    /**
     *
     * @param      newEhcacheStreamMasterIndex  the new object to put in cache
     * @param      failIfNotWritable            if true, method will throw an exception if the cached object is not write-able
     * @return     object previously cached for this key, or null if no Element was cached
     * @exception  IOException  if the replace does not work (eg. something else is writing at the same time)
     *
     */
    private EhcacheStreamMasterIndex casReplaceEhcacheStreamMasterIndex(EhcacheStreamMasterIndex newEhcacheStreamMasterIndex, boolean failIfNotWritable) throws IOException {
        EhcacheStreamMasterIndex currentCacheMasterIndexForKey = null;
        Element cacheElem;
        if(null != (cacheElem = getEhcacheStreamMasterIndexElement())) {
            currentCacheMasterIndexForKey = (EhcacheStreamMasterIndex)cacheElem.getObjectValue();
            if(failIfNotWritable && null != currentCacheMasterIndexForKey && currentCacheMasterIndexForKey.isCurrentWrite())
                throw new IOException("Concurrent write not allowed - Current cache entry with key[" + cacheKey + "] is currently being written...");

            if(null != newEhcacheStreamMasterIndex) {
                //replace old writeable element with new one using CAS operation for consistency
                if (!cache.replace(cacheElem, new Element(buildMasterKey(), newEhcacheStreamMasterIndex))) {
                    throw new IOException("Concurrent write not allowed - Current cache entry with key[" + cacheKey + "] is currently being written...");
                }
            } else { // if null, let's understand this as a remove of current cache value
                if(!cache.removeElement(cacheElem)){
                    throw new IOException("Concurrent write not allowed - Current cache entry with key[" + cacheKey + "] is currently being written...");
                }
            }
        } else {
            if(null != newEhcacheStreamMasterIndex) { //only add a new entry if the object to add is not null...otherwise do nothing
                // add new entry using CAS operation for consistency...
                // if it's not null, it means there was something in there...meaning something has been added since our last write...not good hence exception
                if (null != cache.putIfAbsent(new Element(buildMasterKey(), newEhcacheStreamMasterIndex))) {
                    throw new IOException("Concurrent write not allowed - Current cache entry with key[" + cacheKey + "] is currently being written...");
                }
            }
        }

        return currentCacheMasterIndexForKey;
    }

    /** Flush the internal buffer */
    private void flushBuffer() throws IOException {
        if (count > 0) { // we're going to write here
            //first time writing, so clear all cache entries for that key first (overwriting operation)
            //TODO: suspecting some sort of padlocking here for multi-thread protection...let's investigate later when basic functional is done
            if(null == currentStreamMasterIndex) {
                //set a new EhcacheStreamMasterIndex in write mode
                EhcacheStreamMasterIndex newStreamMasterIndex = new EhcacheStreamMasterIndex(EhcacheStreamMasterIndex.StreamOpStatus.CURRENT_WRITE);

                /*
                TODO let's think about this a bit more: if 2 thread arrive here, 1 should fail and the other should go through...fine.
                TODO But the one which failed is going to try to close the stream, which flush the buffer again...hence potential for overwritting the first one if it's already finished...
                */

                //set a new EhcacheStreamMasterIndex in write mode in cache if current element in cache is writable - else exception (protecting from concurrent writing)
                EhcacheStreamMasterIndex oldEhcacheStreamMasterIndex = casReplaceEhcacheStreamMasterIndex(
                        new EhcacheStreamMasterIndex(EhcacheStreamMasterIndex.StreamOpStatus.CURRENT_WRITE),
                        true);

                //if previous cas operation successful, create a new EhcacheStreamMasterIndex for currentStreamMasterIndex (to avoid soft references issues to the cached value above)
                currentStreamMasterIndex = new EhcacheStreamMasterIndex(EhcacheStreamMasterIndex.StreamOpStatus.CURRENT_WRITE);

                //at this point, we're somewhat safe...entry is set to write-able
                //let's do some cleanup first
                clearChunksForKey(oldEhcacheStreamMasterIndex);
            }

            cache.put(new Element(new EhcacheStreamKey(cacheKey, currentStreamMasterIndex.getAndIncrementChunkIndex()), new EhcacheStreamValue(Arrays.copyOf(buf, count))));
            count = 0; //reset buffer count
        }
    }

    @Override
    public void write(int b) throws IOException {
        if (count >= buf.length) {
            flushBuffer();
        }
        buf[count++] = (byte)b;
    }

    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if (len >= buf.length) {
            //simple implementation...but works
            for(int i=0; i<len;i++){
                write(b[off+i]);
            }
            return;
        }
        if (len > buf.length - count) {
            flushBuffer();
        }
        System.arraycopy(b, off, buf, count, len);
        count += len;
    }

    @Override
    public void flush() throws IOException {
        flushBuffer();
    }

    @Override
    public void close() throws IOException {
        flush();

        //finalize the EhcacheStreamMasterIndex value by saving it in cache
        if(null != currentStreamMasterIndex && currentStreamMasterIndex.isCurrentWrite()) {
            currentStreamMasterIndex.setAvailable();
            casReplaceEhcacheStreamMasterIndex(currentStreamMasterIndex, false);
        }

        currentStreamMasterIndex = null;
    }
}
