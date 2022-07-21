package com.jdcloud.logs.producer.core;

import com.google.common.math.LongMath;
import com.jdcloud.logs.api.LogClient;
import com.jdcloud.logs.api.common.LogItem;
import com.jdcloud.logs.api.exception.LogException;
import com.jdcloud.logs.api.http.error.HttpErrorCode;
import com.jdcloud.logs.api.request.PutLogsRequest;
import com.jdcloud.logs.producer.config.ProducerConfig;
import com.jdcloud.logs.producer.disruptor.LogEvent;
import com.jdcloud.logs.producer.errors.ProducerException;
import com.jdcloud.logs.producer.util.LogSizeCalculator;
import com.jdcloud.logs.producer.util.LogThreadFactory;
import com.jdcloud.logs.producer.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 消费处理
 *
 * @author liubai
 * @date 2022/6/29
 */
public class BatchSender extends AbstractCloser implements Sender<LogEvent, LogProcessor.GroupKey> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BatchSender.class);

    private static final String IO_THREAD_PREFIX = "-sender-";

    private final Map<String, LogClient> logClientPool;

    private final ProducerConfig producerConfig;

    private final ExecutorService sendThreadPool;

    private final RetryQueue retryQueue;

    private final ResourceHolder resourceHolder;

    public BatchSender(Map<String, LogClient> logClientPool, ProducerConfig producerConfig, String threadPrefix,
                       RetryQueue retryQueue, ResourceHolder resourceHolder) {
        this.logClientPool = logClientPool;
        this.producerConfig = producerConfig;
        this.sendThreadPool = new ThreadPoolExecutor(producerConfig.getSendThreads(),
                producerConfig.getSendThreads(), 0, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(), new LogThreadFactory(threadPrefix + IO_THREAD_PREFIX));
        this.retryQueue = retryQueue;
        this.resourceHolder = resourceHolder;
    }

    @Override
    public void send(List<LogEvent> logEvents, LogProcessor.GroupKey groupKey) {
        if (Utils.isEmpty(logEvents)) {
            return;
        }

        List<LogItem> logItems = new LinkedList<LogItem>();
        for (LogEvent logEvent : logEvents) {
            logItems.add(logEvent.getLogItem());
        }
        submit(new SendBatchTask(new LogBatch(logItems, groupKey, LogSizeCalculator.calculate(logEvents)),
                producerConfig, logClientPool, retryQueue, resourceHolder));
    }

    public void submit(SendBatchTask task) {
        sendThreadPool.submit(task);
    }

    @Override
    public void doClose(long timeoutMillis) throws InterruptedException, ProducerException {
        sendThreadPool.shutdown();
        if (sendThreadPool.awaitTermination(timeoutMillis, TimeUnit.MILLISECONDS)) {
            LOGGER.debug("The batchSender is terminated");
        } else {
            LOGGER.warn("The batchSender is not fully terminated");
            throw new ProducerException("the batchSender is not fully terminated");
        }
    }

    public static class SendBatchTask implements Runnable {

        private static final Logger LOGGER = LoggerFactory.getLogger(SendBatchTask.class);

        private final ProducerConfig producerConfig;
        private final Map<String, LogClient> logClientPool;
        private final LogBatch logBatch;
        private final RetryQueue retryQueue;
        private final ResourceHolder resourceHolder;

        public SendBatchTask(LogBatch logBatch, ProducerConfig producerConfig, Map<String, LogClient> logClientPool,
                             RetryQueue retryQueue, ResourceHolder resourceHolder) {
            this.producerConfig = producerConfig;
            this.logClientPool = logClientPool;
            this.logBatch = logBatch;
            this.retryQueue = retryQueue;
            this.resourceHolder = resourceHolder;
        }

        @Override
        public void run() {
            try {
                sendBatch();
            } catch (Throwable t) {
                LOGGER.error("Uncaught error in send batch task, regionId={}, logTopic={}", logBatch.getRegionId(),
                        logBatch.getLogTopic(), t);
            }
        }

        private void sendBatch() {
            LOGGER.trace("Ready to send batch, logBatch={}", logBatch);
            LogClient logClient = getLogClient(logBatch.getRegionId());
            if (logClient == null) {
                LOGGER.error("Failed to get client, regionId={}, logTopic={}", logBatch.getRegionId(),
                        logBatch.getLogTopic());
                release();
            } else {
                try {
                    PutLogsRequest request = buildPutLogsRequest(logBatch);
                    logClient.putLogs(request);
                } catch (Exception e) {
                    LOGGER.error("Failed to put logs, regionId={}, logTopic={}, retries={}", logBatch.getRegionId(),
                            logBatch.getLogTopic(), logBatch.getRetries(), e);
                    if (notRetry(e)) {
                        LOGGER.debug("Do not retry, because the above exception is not a retry exception, " +
                                "or the retry queue is closed, or retries is greater than the configured retries, " +
                                "ready to release, retries={}", logBatch.getRetries());
                        release();
                    } else {
                        long retryBackoffMillis = calculateRetryBackoffMillis();
                        logBatch.setNextRetryMillis(System.currentTimeMillis() + retryBackoffMillis);
                        logBatch.increaseRetries();
                        LOGGER.debug("Ready to put batch to retry queue, retryBackoffMillis={}, next retries={}",
                                retryBackoffMillis, logBatch.getRetries());
                        try {
                            retryQueue.put(logBatch);
                        } catch (IllegalStateException e1) {
                            LOGGER.error("Failed to put batch to the retry queue since the retry queue was closed, " +
                                    "ready to release");
                            release();
                        }
                    }
                    return;
                }
                release();
                LOGGER.trace("Send batch successfully, batch={}", logBatch);
            }
        }

        private LogClient getLogClient(String regionId) {
            return logClientPool.get(regionId);
        }

        private void release() {
            resourceHolder.release(logBatch.getBatchCount(), logBatch.getBatchSizeInBytes());
        }

        private PutLogsRequest buildPutLogsRequest(LogBatch logBatch) {
            return new PutLogsRequest(logBatch.getLogTopic(), logBatch.getLogItems(), logBatch.getSource(),
                    logBatch.getFileName());
        }

        private boolean notRetry(Throwable e) {
            if (!isRetrieableException(e)) {
                return true;
            }
            if (retryQueue.isClosed()) {
                return true;
            }
            return logBatch.getRetries() >= producerConfig.getRetries();
        }

        private boolean isRetrieableException(Throwable e) {
            if (e instanceof LogException) {
                LogException logException = (LogException) e;
                return (logException.getErrorCode().equals(HttpErrorCode.CONNECTION_TIMEOUT)
                        || logException.getErrorCode().equals(HttpErrorCode.SOCKET_TIMEOUT));
            }
            return false;
        }

        private long calculateRetryBackoffMillis() {
            int retry = logBatch.getRetries();
            long retryBackoffMs = producerConfig.getInitRetryBackoffMillis() * LongMath.pow(2, retry);
            if (retryBackoffMs <= 0) {
                retryBackoffMs = producerConfig.getMaxRetryBackoffMillis();
            }
            return Math.min(retryBackoffMs, producerConfig.getMaxRetryBackoffMillis());
        }
    }
}