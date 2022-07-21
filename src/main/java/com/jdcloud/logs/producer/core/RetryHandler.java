package com.jdcloud.logs.producer.core;

import com.jdcloud.logs.api.LogClient;
import com.jdcloud.logs.producer.config.ProducerConfig;
import com.jdcloud.logs.producer.errors.ProducerException;
import com.jdcloud.logs.producer.util.LogThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 失败重试
 *
 * @author liubai
 * @date 2022/6/30
 */
public class RetryHandler extends AbstractCloser {

    private static final Logger LOGGER = LoggerFactory.getLogger(RetryHandler.class);

    private final ProducerConfig producerConfig;

    private final Map<String, LogClient> logClientPool;

    private final RetryQueue retryQueue;

    private final BatchSender batchSender;

    private final ResourceHolder resourceHolder;

    private volatile boolean closed;

    private final LogThread logThread;

    public RetryHandler(String producerName, ProducerConfig producerConfig, Map<String, LogClient> logClientPool,
                        RetryQueue retryQueue, BatchSender batchSender, ResourceHolder resourceHolder) {
        this.producerConfig = producerConfig;
        this.logClientPool = logClientPool;
        this.retryQueue = retryQueue;
        this.batchSender = batchSender;
        this.resourceHolder = resourceHolder;
        this.logThread = new LogThread(producerName, true) {
            @Override
            public void run() {
                retry();
            }
        };
        this.closed = false;
    }

    public void start() {
        logThread.start();
    }

    private void retry() {
        loopRetryBatches();
        LOGGER.debug("Ready to shutdown retry thread");
        List<LogBatch> backlogBatches = backlogBatches();
        LOGGER.debug("Submit backlog batches, size={}", backlogBatches.size());
        submitBacklogBatches(backlogBatches);
        LOGGER.debug("Shutdown retry thread completed");
    }

    private void loopRetryBatches() {
        while (!closed) {
            try {
                moveBatch();
            } catch (Exception e) {
                LOGGER.error("Uncaught exception in retry handler", e);
            }
        }
    }

    private void moveBatch() {
        LOGGER.trace("Ready to move expired batch from retry queue to batch sender");
        LogBatch expiredRetryBatch = retryQueue.takeExpired();
        if (expiredRetryBatch == null) {
            return;
        }
        batchSender.submit(createSendBatchTask(expiredRetryBatch));
        LOGGER.trace("Move expired batch successfully, logTopic={}, source={}, logCount={}, sizeInBytes={}",
                expiredRetryBatch.getLogTopic(), expiredRetryBatch.getSource(),
                expiredRetryBatch.getBatchCount(), expiredRetryBatch.getBatchSizeInBytes());
    }

    private List<LogBatch> backlogBatches() {
        return retryQueue.remainingBatches();
    }

    private void submitBacklogBatches(List<LogBatch> backlogBatches) {
        for (LogBatch b : backlogBatches) {
            batchSender.submit(createSendBatchTask(b));
        }
    }

    private BatchSender.SendBatchTask createSendBatchTask(LogBatch batch) {
        return new BatchSender.SendBatchTask(batch, producerConfig, logClientPool, retryQueue, resourceHolder);
    }

    @Override
    public void doClose(long timeoutMillis) throws InterruptedException, ProducerException {
        this.closed = true;
        logThread.interrupt();
        logThread.join(timeoutMillis);
        if (logThread.isAlive()) {
            LOGGER.warn("The retry handler thread is still alive");
            throw new ProducerException("the retry handler thread is still alive");
        }
    }
}
