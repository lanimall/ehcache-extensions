package com.softwareag.terracotta;

import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;

import java.io.IOException;

/**
 * Created by FabienSanglier on 5/6/15.
 */
public class EhcacheStreamsDAL {
    /*
     * The Internal Ehcache cache object
     */
    private final Cache cache;

    /*
     * The Ehcache cache key object the data should get written to
     */
    private final Object cacheKey;

    EhcacheStreamsDAL(Cache cache, Object cacheKey) {
        this.cache = cache;
        this.cacheKey = cacheKey;
    }

    private EhcacheStreamKey buildMasterKey(){
        return buildChunkKey(EhcacheStreamKey.MASTER_INDEX);
    }

    private EhcacheStreamKey buildChunkKey(int chunkIndex){
        return new EhcacheStreamKey(cacheKey, chunkIndex);
    }

    Element getMasterIndexElement() {
        return cache.get(buildMasterKey());
    }

    Element getChunkElement(int chunkIndex) {
        return cache.get(buildChunkKey(chunkIndex));
    }

    EhcacheStreamValue getChunkValue(int chunkIndex){
        EhcacheStreamValue chunkValue = null;
        Element chunkElem;
        if(null != (chunkElem = getChunkElement(chunkIndex)))
            chunkValue = (EhcacheStreamValue)chunkElem.getObjectValue();

        return chunkValue;
    }

    private EhcacheStreamMasterIndex getMasterIndexValue(){
        EhcacheStreamMasterIndex cacheMasterIndexValue = null;
        Element masterIndexElement = null;
        if(null != (masterIndexElement = getMasterIndexElement())) {
            cacheMasterIndexValue = (EhcacheStreamMasterIndex)masterIndexElement.getObjectValue();
        }

        return cacheMasterIndexValue;
    }

    EhcacheStreamMasterIndex getMasterIndexValueIfAvailable() throws IOException {
        return getMasterIndexValueIfAvailable(getMasterIndexValue());
    }

    EhcacheStreamMasterIndex getMasterIndexValueIfAvailable(EhcacheStreamMasterIndex cacheMasterIndexValue) throws IOException {
        return getMasterIndexValueIfAvailable(cacheMasterIndexValue, true);
    }

    EhcacheStreamMasterIndex getMasterIndexValueIfAvailable(EhcacheStreamMasterIndex cacheMasterIndexValue, boolean failIfNotWritable) throws IOException {
        if(failIfNotWritable && null != cacheMasterIndexValue && cacheMasterIndexValue.isCurrentWrite())
            throw new IOException("Operation not allowed - Current cache entry with key[" + cacheKey + "] is currently being written...");

        return cacheMasterIndexValue;
    }

    void putChunkValue(int chunkIndex, byte[] chunkPayload){
        cache.put(new Element(new EhcacheStreamKey(cacheKey, chunkIndex), new EhcacheStreamValue(chunkPayload)));
    }

    boolean clearChunksForKey(EhcacheStreamMasterIndex ehcacheStreamMasterIndex) {
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

    /**
     *
     * @param      newEhcacheStreamMasterIndex  the new object to put in cache
     * @param      failIfNotWritable            if true, method will throw an exception if the cached object is not write-able
     * @return     object previously cached for this key, or null if no Element was cached
     * @exception  IOException  if the replace does not work (eg. something else is writing at the same time)
     *
     */
     EhcacheStreamMasterIndex casReplaceEhcacheStreamMasterIndex(EhcacheStreamMasterIndex newEhcacheStreamMasterIndex, boolean failIfNotWritable) throws IOException {
        EhcacheStreamMasterIndex currentCacheMasterIndexForKey = null;
        Element cacheElem;
        if(null != (cacheElem = getMasterIndexElement())) {
            currentCacheMasterIndexForKey = getMasterIndexValueIfAvailable((EhcacheStreamMasterIndex)cacheElem.getObjectValue(), failIfNotWritable);
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
}