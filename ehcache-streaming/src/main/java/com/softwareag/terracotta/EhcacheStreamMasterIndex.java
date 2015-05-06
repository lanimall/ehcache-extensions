package com.softwareag.terracotta;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Fabien Sanglier on 5/6/15.
 */
class EhcacheStreamMasterIndex implements Serializable {
    private static final long serialVersionUID = 1L;

    private final AtomicInteger numberOfChunks = new AtomicInteger(0);
    private StreamOpStatus status = StreamOpStatus.AVAILABLE;

    EhcacheStreamMasterIndex() {
        this.status = StreamOpStatus.AVAILABLE;
    }

    EhcacheStreamMasterIndex(StreamOpStatus status) {
        this.status = status;
    }

    public enum StreamOpStatus {
        CURRENT_WRITE, AVAILABLE
    }

    public int getAndIncrementChunkIndex(){
        return numberOfChunks.getAndIncrement();
    }

    public int getNumberOfChunk(){
        return numberOfChunks.get();
    }

    public boolean isCurrentWrite(){
        return status == StreamOpStatus.CURRENT_WRITE;
    }

    public synchronized void setCurrentWrite() {
        this.status = StreamOpStatus.CURRENT_WRITE;
    }

    public synchronized void setAvailable() {
        this.status = StreamOpStatus.AVAILABLE;
    }
}