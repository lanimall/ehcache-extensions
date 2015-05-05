package com.softwareag.terracotta;

import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Arrays;

/**
 * Created by FabienSanglier on 5/4/15.
 */
public class EhcacheOutputStream extends OutputStream {
    private static int DEFAULT_BUFFER_SIZE = 1 * 1024 * 1024; // 1MB
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
    protected volatile int cacheValueChunkIndex = 0;

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

        //start by making sure a cache entry exist for that key so that we can safely use the CAS "replace" operations later
        if(null == cache.get(cacheKey))
            cache.put(new Element(cacheKey, new Integer(0)));
    }

    /** Flush the internal buffer */
    private void flushBuffer() throws IOException {
        if (count > 0) {
            cache.put(new Element(new InnerCacheKey(cacheKey, cacheValueChunkIndex), Arrays.copyOf(buf, count)));
            cacheValueChunkIndex++;
            cache.replace(new Element(cacheKey, new Integer(cacheValueChunkIndex)));
            count = 0;
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
        try {
            flush();
            cacheValueChunkIndex = 0;
        } catch (IOException ignored) {

        }
    }

    static class InnerCacheKey implements Serializable {
        private static final long serialVersionUID = 1L;

        private final Object cacheKey;
        private final int index;

        InnerCacheKey(Object cacheKey, int index) {
            this.cacheKey = cacheKey;
            this.index = index;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            InnerCacheKey that = (InnerCacheKey) o;

            if (index != that.index) return false;
            if (cacheKey != null ? !cacheKey.equals(that.cacheKey) : that.cacheKey != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = cacheKey != null ? cacheKey.hashCode() : 0;
            result = 31 * result + index;
            return result;
        }

        @Override
        public String toString() {
            return "InnerCacheKey{" +
                    "cacheKey=" + cacheKey +
                    ", index=" + index +
                    '}';
        }
    }
}
