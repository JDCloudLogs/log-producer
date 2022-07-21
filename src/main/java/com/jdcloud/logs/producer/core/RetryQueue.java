package com.jdcloud.logs.producer.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 失败重试
 *
 * @author liubai
 * @date 2022/6/30
 */
public class RetryQueue extends AbstractCloser {

    private static final Logger LOGGER = LoggerFactory.getLogger(RetryQueue.class);

    private final DelayQueue<LogBatch> retryBatches = new DelayQueue<LogBatch>();

    private final AtomicInteger putsInProgress;

    private volatile boolean closed;

    public RetryQueue() {
        this.putsInProgress = new AtomicInteger(0);
        this.closed = false;
    }

    public void put(LogBatch batch) {
        putsInProgress.incrementAndGet();
        try {
            if (closed) {
                throw new IllegalStateException("Cannot put after the retry queue was closed");
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
        retryBatches.clear();
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
