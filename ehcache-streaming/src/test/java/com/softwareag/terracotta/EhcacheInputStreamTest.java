package com.softwareag.terracotta;

import net.sf.ehcache.Element;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.nio.file.Files;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

public class EhcacheInputStreamTest extends EhcacheStreamingTestsBase {

    private long fileCheckSum = -1L;

    @Before
    public void copyFileToCache() throws Exception {
        int inBufferSize = 32 * 1024;
        int outBufferSize = 253 * 1024; // cache entries are going to be around 253kb
        int copyBufferSize = 128 * 1024;

        try (
                CheckedInputStream is = new CheckedInputStream(new BufferedInputStream(Files.newInputStream(IN_FILE_PATH),inBufferSize),new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(new EhcacheOutputStream(cache, cache_key, outBufferSize),new CRC32());
        )
        {
            System.out.println("============ copyFileToCache ====================");

            long start = System.currentTimeMillis();
            pipeStreamsWithBuffer(is, os, copyBufferSize);
            long end = System.currentTimeMillis();

            System.out.println("Execution Time = " + (end - start) + " millis");
            System.out.println("============================================");

            this.fileCheckSum = is.getChecksum().getValue();
            Assert.assertEquals(is.getChecksum().getValue(), os.getChecksum().getValue());
        }
    }

    @Test
    public void copyCacheToFileUsingNativeCacheCalls() throws Exception {
        int outBufferSize = 32 * 1024;
        try (
                CheckedOutputStream os = new CheckedOutputStream(new BufferedOutputStream(Files.newOutputStream(OUT_FILE_PATH),outBufferSize), new CRC32());
        )
        {
            System.out.println("============ copyCacheToFileUsingNativeCacheCalls ====================");

            CRC32 cacheCheckSum = new CRC32();

            Element cacheValue = null;
            if(null == (cacheValue = cache.get(cache_key)))
                throw new Exception(cache_key + " not found");

            long start = System.currentTimeMillis();
            Integer totalChunks = (Integer)cacheValue.getObjectValue();
            System.out.println("Total Chunks = " + totalChunks);
            Element chunkElem;
            for(int i = 0; i < totalChunks; i++){
                if(null == (chunkElem = cache.get(new EhcacheOutputStream.InnerCacheKey(cache_key, i))))
                    throw new Exception(new EhcacheOutputStream.InnerCacheKey(cache_key, i).toString() + " not found");

                byte[] buffer = (byte[])chunkElem.getObjectValue();
                cacheCheckSum.update(buffer); // update cache checksum

                os.write(buffer);

            }
            long end = System.currentTimeMillis();

            System.out.println("Execution Time = " + (end - start) + " millis");
            System.out.println("============================================");

            Assert.assertEquals(fileCheckSum, os.getChecksum().getValue());
            Assert.assertEquals(cacheCheckSum.getValue(), fileCheckSum);
            Assert.assertEquals(cacheCheckSum.getValue(), os.getChecksum().getValue());
        }
    }

    @Test
    public void copyCacheToFileUsingStreamSmallerCopyBuffer() throws Exception {
        int inBufferSize = 128 * 1024; //ehcache input stream internal buffer
        int outBufferSize = 128 * 1024;
        int copyBufferSize = 64 * 1024; //copy buffer size *smaller* than ehcache input stream internal buffer to make sure it works that way

        try (
                CheckedInputStream is = new CheckedInputStream(new EhcacheInputStream(cache, cache_key, inBufferSize),new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(new BufferedOutputStream(Files.newOutputStream(OUT_FILE_PATH),outBufferSize), new CRC32());
        )
        {
            System.out.println("============ copyCacheToFileUsingStreamSmallerCopyBuffer ====================");
            long start = System.currentTimeMillis();
            pipeStreamsWithBuffer(is, os, copyBufferSize);
            long end = System.currentTimeMillis();

            System.out.println("Execution Time = " + (end - start) + " millis");
            System.out.println("============================================");

            Assert.assertEquals(fileCheckSum, os.getChecksum().getValue());
            Assert.assertEquals(is.getChecksum().getValue(), fileCheckSum);
            Assert.assertEquals(is.getChecksum().getValue(), os.getChecksum().getValue());
        }
    }

    @Test
    public void copyCacheToFileUsingStreamLargerCopyBuffer() throws Exception {
        int inBufferSize = 128 * 1024; //ehcache input stream internal buffer
        int outBufferSize = 128 * 1024;
        int copyBufferSize = 357 * 1024; //copy buffer size *larger* than ehcache input stream internal buffer to make sure it works that way

        try (
                CheckedInputStream is = new CheckedInputStream(new EhcacheInputStream(cache, cache_key, inBufferSize),new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(new BufferedOutputStream(Files.newOutputStream(OUT_FILE_PATH),outBufferSize), new CRC32());
        )
        {
            System.out.println("============ copyCacheToFileUsingStreamLargerCopyBuffer ====================");
            long start = System.currentTimeMillis();
            pipeStreamsWithBuffer(is, os, copyBufferSize);
            long end = System.currentTimeMillis();

            System.out.println("Execution Time = " + (end - start) + " millis");
            System.out.println("============================================");

            Assert.assertEquals(fileCheckSum, os.getChecksum().getValue());
            Assert.assertEquals(is.getChecksum().getValue(), fileCheckSum);
            Assert.assertEquals(is.getChecksum().getValue(), os.getChecksum().getValue());
        }
    }

    @Test
    public void copyCacheToFileUsingStreamDefaultBuffers() throws Exception {
        int copyBufferSize = 512 * 1024; //copy buffer size

        try (
                CheckedInputStream is = new CheckedInputStream(new EhcacheInputStream(cache, cache_key),new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(new BufferedOutputStream(Files.newOutputStream(OUT_FILE_PATH)), new CRC32());
        )
        {
            System.out.println("============ copyCacheToFileUsingStreamDefaultBuffers ====================");
            long start = System.currentTimeMillis();
            pipeStreamsWithBuffer(is, os, copyBufferSize);
            long end = System.currentTimeMillis();

            System.out.println("Execution Time = " + (end - start) + " millis");
            System.out.println("============================================");

            Assert.assertEquals(fileCheckSum, os.getChecksum().getValue());
            Assert.assertEquals(is.getChecksum().getValue(), fileCheckSum);
            Assert.assertEquals(is.getChecksum().getValue(), os.getChecksum().getValue());
        }
    }

    @Test
    public void copyCacheToFileUsingStreamDefaultBuffersByteByByte() throws Exception {
        try (
                CheckedInputStream is = new CheckedInputStream(new EhcacheInputStream(cache, cache_key),new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(new BufferedOutputStream(Files.newOutputStream(OUT_FILE_PATH)), new CRC32());
        )
        {
            System.out.println("============ copyCacheToFileUsingStreamDefaultBuffersByteByByte ====================");
            long start = System.currentTimeMillis();
            pipeStreamsByteByByte(is, os);
            long end = System.currentTimeMillis();

            System.out.println("Execution Time = " + (end - start) + " millis");
            System.out.println("============================================");

            Assert.assertEquals(fileCheckSum, os.getChecksum().getValue());
            Assert.assertEquals(is.getChecksum().getValue(), fileCheckSum);
            Assert.assertEquals(is.getChecksum().getValue(), os.getChecksum().getValue());
        }
    }

    @Test
    public void copyCacheToFileNoCacheKey() throws Exception {
        int copyBufferSize = 512 * 1024; //copy buffer size
        final String cacheKey = "something-else";
        try (
                CheckedInputStream is = new CheckedInputStream(new EhcacheInputStream(cache, cacheKey),new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(new BufferedOutputStream(Files.newOutputStream(OUT_FILE_PATH)), new CRC32());
        )
        {
            System.out.println("============ copyCacheToFileUsingStreamDefaultBuffers ====================");
            long start = System.currentTimeMillis();
            pipeStreamsWithBuffer(is, os, copyBufferSize);
            long end = System.currentTimeMillis();

            System.out.println("Execution Time = " + (end - start) + " millis");
            System.out.println("============================================");

            Assert.assertEquals(0, os.getChecksum().getValue());
            Assert.assertEquals(is.getChecksum().getValue(), 0);
            Assert.assertEquals(is.getChecksum().getValue(), os.getChecksum().getValue());
        }
    }
}