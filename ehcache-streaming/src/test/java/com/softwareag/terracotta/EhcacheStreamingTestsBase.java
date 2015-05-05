package com.softwareag.terracotta;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheException;
import net.sf.ehcache.CacheManager;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;

/**
 * Created by FabienSanglier on 5/5/15.
 */
public abstract class EhcacheStreamingTestsBase {
    protected static final int IN_FILE_SIZE = 200 * 1024 * 1024;
    protected static final Path TESTS_DIR_PATH = FileSystems.getDefault().getPath(System.getProperty("java.io.tmpdir"));
    protected static final Path IN_FILE_PATH = FileSystems.getDefault().getPath(TESTS_DIR_PATH.toString(),"sample_big_file_in.txt");
    protected static final Path OUT_FILE_PATH = FileSystems.getDefault().getPath(TESTS_DIR_PATH.toString(), "sample_big_file_out.txt");

    protected Cache cache;
    protected static final String cache_key = "some_key";

    @BeforeClass
    public static void oneTimeSetup() throws Exception {
        CacheManager.getInstance();
        generateBigFile();
    }

    private static void generateBigFile() throws Exception {
        try (
                CheckedOutputStream os = new CheckedOutputStream(new BufferedOutputStream(Files.newOutputStream(IN_FILE_PATH)), new CRC32());
        ) {
            System.out.println("============ Generate Initial Big File ====================");

            long start = System.currentTimeMillis();
            int size = IN_FILE_SIZE;
            for (int i = 0; i < size; i++) {
                os.write(i);
            }
            long end = System.currentTimeMillis();

            System.out.println("Execution Time = " + (end - start) + " millis");
            System.out.println("CheckSum = " + os.getChecksum().getValue());
            System.out.println("============================================");
        }
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        //remove files
        Files.delete(IN_FILE_PATH);
        Files.delete(OUT_FILE_PATH);
        CacheManager.getInstance().shutdown();
    }

    @Before
    public void setUp() throws Exception {
        String cacheName = "FileStore";
        try {
            cache = CacheManager.getInstance().getCache(cacheName);
        } catch (IllegalStateException e) {
            e.printStackTrace();
        } catch (ClassCastException e) {
            e.printStackTrace();
        } catch (CacheException e) {
            e.printStackTrace();
        }

        if (cache == null) {
            throw new IllegalArgumentException("Could not find the cache " + cacheName);
        }
    }

    protected void pipeStreamsByteByByte(InputStream is, OutputStream os) throws IOException {
        int n;
        while ((n = is.read()) > -1) {
            os.write(n);
        }
    }

    protected void pipeStreamsWithBuffer(InputStream is, OutputStream os, int bufferSize) throws IOException {
        int n;
        byte[] buffer = new byte[bufferSize];
        while ((n = is.read(buffer)) > -1) {
            os.write(buffer, 0, n);   // Don't allow any extra bytes to creep in, final write
        }
    }
}
