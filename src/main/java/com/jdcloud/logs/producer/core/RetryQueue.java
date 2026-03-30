package com.jdcloud.logs.producer.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 失败重试（带容量限制，避免堆积导致OOM）
 *
 * @author liubai
 * @date 2022/6/30
 */
public class RetryQueue extends AbstractCloser {

    private static final Logger LOGGER = LoggerFactory.getLogger(RetryQueue.class);

    private final DelayQueue<LogBatch> retryBatches = new DelayQueue<LogBatch>();

    private final AtomicInteger putsInProgress;

    private final Semaphore capacityController;

    private volatile boolean closed;

    public RetryQueue(int capacity) {
        this.putsInProgress = new AtomicInteger(0);
        this.capacityController = new Semaphore(capacity);
        this.closed = false;
    }

    public void put(LogBatch batch) {
        putsInProgress.incrementAndGet();
        try {
            if (closed) {
                throw new IllegalStateException("Cannot put after the retry queue was closed");
            }
            // 容量限制，防止重试队列无限增长
            if (!capacityController.tryAcquire()) {
                throw new IllegalStateException("Retry queue capacity exceeded");
            }
            retryBatches.put(batch);
        } finally {
            putsInProgress.decrementAndGet();
        }
    }

    public LogBatch takeExpired() {
        LogBatch batch = null;
        try {
            batch = retryBatches.take();
        } catch (InterruptedException e) {
            LOGGER.info("Interrupted when take batch from the retry batches");
        }
        if (batch != null) {
            capacityController.release();
        }
        return batch;
    }

    public List<LogBatch> remainingBatches() {
        if (!closed) {
            throw new IllegalStateException("Cannot get the remaining batches before the retry queue closed");
        }
        while (true) {
            if (!putsInProgress()) {
                break;
            }
        }
        List<LogBatch> remainingBatches = new ArrayList<LogBatch>(retryBatches);
        int size = remainingBatches.size();
        retryBatches.clear();
        // 清空时释放占用的容量
        if (size > 0) {
            capacityController.release(size);
        }
        return remainingBatches;
    }

    public boolean isClosed() {
        return closed;
    }

    private boolean putsInProgress() {
        return putsInProgress.get() > 0;
    }

    @Override
    public void doClose(long timeoutMillis) {
        this.closed = true;
    }
}
