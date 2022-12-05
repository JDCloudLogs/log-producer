package com.jdcloud.logs.producer.core;

import com.google.common.math.LongMath;
import com.jdcloud.logs.api.LogClient;
import com.jdcloud.logs.api.exception.LogException;
import com.jdcloud.logs.api.http.error.HttpErrorCode;
import com.jdcloud.logs.api.request.PutLogsRequest;
import com.jdcloud.logs.api.response.PutLogsResponse;
import com.jdcloud.logs.producer.config.ProducerConfig;
import com.jdcloud.logs.producer.disruptor.LogEvent;
import com.jdcloud.logs.producer.errors.ProducerException;
import com.jdcloud.logs.producer.res.Attempt;
import com.jdcloud.logs.producer.res.ResCode;
import com.jdcloud.logs.producer.util.LogThreadFactory;
import com.jdcloud.logs.producer.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;

/**
 * 消费处理
 *
 * @author liubai
 * @date 2022/6/29
 */
public class BatchSender extends AbstractCloser implements Sender<LogEvent> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BatchSender.class);

    private static final String IO_THREAD_PREFIX = "-sender-";

    private final Map<String, LogClient> logClientPool;

    private final ProducerConfig producerConfig;

    private final ExecutorService sendThreadPool;

    private final RetryQueue retryQueue;

    private final ResourceHolder resourceHolder;

    private final BlockingQueue<LogBatch> successQueue;

    private final BlockingQueue<LogBatch> failureQueue;

    public BatchSender(Map<String, LogClient> logClientPool, ProducerConfig producerConfig, String threadPrefix,
                       RetryQueue retryQueue, ResourceHolder resourceHolder,
                       BlockingQueue<LogBatch> successQueue, BlockingQueue<LogBatch> failureQueue) {
        this.logClientPool = logClientPool;
        this.producerConfig = producerConfig;
        this.sendThreadPool = new ThreadPoolExecutor(producerConfig.getSendThreads(),
                producerConfig.getSendThreads(), 0, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(), new LogThreadFactory(threadPrefix + IO_THREAD_PREFIX));
        this.retryQueue = retryQueue;
        this.resourceHolder = resourceHolder;
        this.successQueue = successQueue;
        this.failureQueue = failureQueue;
    }

    @Override
    public void send(LogBatch logBatch) {
        if (logBatch == null || logBatch.getBatchCount() == 0) {
            return;
        }

        submit(new SendBatchTask(logBatch, producerConfig, logClientPool, retryQueue, resourceHolder,
                successQueue, failureQueue));
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
        private final BlockingQueue<LogBatch> successQueue;
        private final BlockingQueue<LogBatch> failureQueue;

        public SendBatchTask(LogBatch logBatch, ProducerConfig producerConfig, Map<String, LogClient> logClientPool,
                             RetryQueue retryQueue, ResourceHolder resourceHolder,
                             BlockingQueue<LogBatch> successQueue, BlockingQueue<LogBatch> failureQueue) {
            this.producerConfig = producerConfig;
            this.logClientPool = logClientPool;
            this.logBatch = logBatch;
            this.retryQueue = retryQueue;
            this.resourceHolder = resourceHolder;
            this.successQueue = successQueue;
            this.failureQueue = failureQueue;
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

        private void sendBatch() throws InterruptedException {
            LOGGER.trace("Ready to send batch, logBatch={}", logBatch);
            long timestampMills = System.currentTimeMillis();
            LogClient logClient = getLogClient(logBatch.getRegionId());
            if (logClient == null) {
                LOGGER.error("Log client not found, regionId={}, logTopic={}", logBatch.getRegionId(),
                        logBatch.getLogTopic());
                Attempt attempt = new Attempt(false, "", ResCode.LOG_CLIENT_NOT_FOUNT,
                        "Log client not found", timestampMills);
                logBatch.addAttempt(attempt);
                failureQueue.put(logBatch);
            } else {
                PutLogsResponse response;
                try {
                    PutLogsRequest request = buildPutLogsRequest(logBatch);
                    response = logClient.putLogs(request);
                } catch (Exception e) {
                    logBatch.addAttempt(createErrorAttempt(e, timestampMills));
                    LOGGER.error("Failed to put logs, regionId={}, logTopic={}, attemptCount={}", logBatch.getRegionId(),
                            logBatch.getLogTopic(), logBatch.getAttemptCount(), e);
                    if (notRetry(e)) {
                        LOGGER.debug("Do not retry, because the above exception is not a retry exception, " +
                                "or the retry queue is closed, or retries is greater than the configured retries, " +
                                "ready to release, retries={}", logBatch.getRetries());
                        failureQueue.put(logBatch);
                    } else {
                        long retryBackoffMillis = calculateRetryBackoffMillis();
                        logBatch.setNextRetryMillis(System.currentTimeMillis() + retryBackoffMillis);
                        LOGGER.debug("Ready to put batch to retry queue, retryBackoffMillis={}, next retries={}",
                                retryBackoffMillis, logBatch.getAttemptCount());
                        try {
                            retryQueue.put(logBatch);
                        } catch (IllegalStateException e1) {
                            LOGGER.error("Failed to put batch to the retry queue since the retry queue was closed, " +
                                    "ready to release");
                            failureQueue.put(logBatch);
                        }
                    }
                    return;
                }
                Attempt attempt = new Attempt(true, response.getRequestId(), ResCode.SUCCESSFUL,
                        "Send successfully", System.currentTimeMillis());
                logBatch.addAttempt(attempt);
                successQueue.put(logBatch);
                LOGGER.trace("Send batch successfully, batch={}", logBatch);
            }
        }

        private Attempt createErrorAttempt(Exception e, long timestampMills) {
            String requestId;
            String errorCode;
            if (e instanceof LogException) {
                LogException logException = (LogException) e;
                requestId = logException.getRequestId();
                if (HttpErrorCode.HTTP_RESPONSE_EXCEPTION.equals(logException.getErrorCode())) {
                    errorCode = String.valueOf(logException.getHttpCode());
                } else {
                    errorCode = logException.getErrorCode();
                }
            } else {
                requestId = "";
                errorCode = ResCode.SEND_ERROR;
            }
            return new Attempt(false, requestId, errorCode, e.getMessage(), timestampMills);
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