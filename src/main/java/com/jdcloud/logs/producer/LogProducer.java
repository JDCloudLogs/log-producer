package com.jdcloud.logs.producer;

import com.google.common.util.concurrent.ListenableFuture;
import com.jdcloud.logs.api.LogClient;
import com.jdcloud.logs.api.common.LogItem;
import com.jdcloud.logs.api.config.ClientConfig;
import com.jdcloud.logs.api.util.Validate;
import com.jdcloud.logs.producer.config.ProducerConfig;
import com.jdcloud.logs.producer.config.RegionConfig;
import com.jdcloud.logs.producer.core.*;
import com.jdcloud.logs.producer.errors.MaxBatchSizeExceedException;
import com.jdcloud.logs.producer.errors.ProducerException;
import com.jdcloud.logs.producer.res.Response;
import com.jdcloud.logs.producer.res.ResponseHandler;
import com.jdcloud.logs.producer.util.LogThreadFactory;
import com.jdcloud.logs.producer.util.LogUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * log producer
 *
 * @author liubai
 * @date 2022/6/29
 */
public class LogProducer implements Producer {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogProducer.class);

    private static final AtomicInteger INSTANCE_ID_GENERATOR = new AtomicInteger(0);

    private static final String LOG_PRODUCER_PREFIX = "jdcloud-log-producer-";

    private static final String RETRY_HANDLER_EXECUTOR_SUFFIX = "-retry-handler";

    private static final String SUCCESS_RESPONSE_HANDLER_FLAG = "success";

    private static final String FAILURE_RESPONSE_HANDLER_FLAG = "failure";

    private static final String HTTP_EXECUTOR_SUFFIX = "-http-";

    private final ProducerConfig producerConfig;

    private final ThreadPoolExecutor httpExecutor;

    private final Map<String, LogClient> logClientPool = new ConcurrentHashMap<String, LogClient>();

    private final ResourceHolder resourceHolder;

    private final RetryQueue retryQueue;

    private final BatchSender batchSender;

    private final LogProcessor logProcessor;

    private final RetryHandler retryHandler;

    private final ResponseHandler successHandler;

    private final ResponseHandler failureHandler;

    public LogProducer(ProducerConfig producerConfig) {
        int instanceId = INSTANCE_ID_GENERATOR.getAndIncrement();
        String producerName = LOG_PRODUCER_PREFIX + instanceId;
        this.producerConfig = producerConfig;
        LogUtils.LOG_CONVERGENCE = producerConfig.isLogConvergence();
        LogUtils.CONVERGENCE_MILLIS = producerConfig.getLogConvergenceMillis();
        this.resourceHolder = new ResourceHolder(producerConfig.getTotalSizeInBytes());
        this.httpExecutor = new ThreadPoolExecutor(producerConfig.getSendThreads(),
                producerConfig.getSendThreads(), 0, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(), new LogThreadFactory(producerName + HTTP_EXECUTOR_SUFFIX));
        this.retryQueue = new RetryQueue();
        BlockingQueue<LogBatch> successQueue = new LinkedBlockingQueue<LogBatch>();
        BlockingQueue<LogBatch> failureQueue = new LinkedBlockingQueue<LogBatch>();
        this.batchSender = new BatchSender(this.logClientPool, producerConfig, producerName, this.retryQueue,
                resourceHolder, successQueue, failureQueue);
        this.logProcessor = new LogProcessor(this.batchSender, producerConfig, resourceHolder, producerName);
        this.retryHandler = new RetryHandler(producerName + RETRY_HANDLER_EXECUTOR_SUFFIX, producerConfig,
                this.logClientPool, this.retryQueue, this.batchSender, resourceHolder, successQueue,
                failureQueue);
        this.successHandler = new ResponseHandler(successQueue, producerName, SUCCESS_RESPONSE_HANDLER_FLAG, resourceHolder);
        this.failureHandler = new ResponseHandler(failureQueue, producerName, FAILURE_RESPONSE_HANDLER_FLAG, resourceHolder);
        this.retryHandler.start();
        this.successHandler.start();
        this.failureHandler.start();
    }

    @Override
    public ListenableFuture<Response> send(String regionId, String logTopic, LogItem logItem)
            throws ProducerException, InterruptedException {
        return send(regionId, logTopic, logItem, null, null);
    }

    @Override
    public ListenableFuture<Response> send(String regionId, String logTopic, LogItem logItem, String source, String fileName)
            throws ProducerException, InterruptedException {
        Validate.notNull(logItem, "logItem");
        List<LogItem> logItems = new ArrayList<LogItem>();
        logItems.add(logItem);
        return send(regionId, logTopic, logItems, source, fileName);
    }

    @Override
    public ListenableFuture<Response> send(String regionId, String logTopic, List<LogItem> logItems)
            throws ProducerException, InterruptedException {
        return send(regionId, logTopic, logItems, null, null);
    }

    @Override
    public ListenableFuture<Response> send(String regionId, String logTopic, List<LogItem> logItems, String source, String fileName)
            throws ProducerException, InterruptedException {
        Validate.notBlank(logTopic, "logTopic");
        Validate.notNull(logItems, "logItems");
        if (logItems.isEmpty()) {
            throw new IllegalArgumentException("logItems cannot be empty");
        }
        int count = logItems.size();
        if (count > ProducerConfig.MAX_BATCH_SIZE) {
            throw new MaxBatchSizeExceedException("the log list size is " + count
                    + " which exceeds the MAX_BATCH_COUNT " + ProducerConfig.MAX_BATCH_SIZE);
        }
        return logProcessor.process(regionId, logTopic, logItems, source, fileName);
    }

    @Override
    public void close() throws InterruptedException, ProducerException {
        close(Long.MAX_VALUE);
    }

    @Override
    public void close(long timeoutMillis) throws InterruptedException, ProducerException {
        if (timeoutMillis < 0) {
            throw new IllegalArgumentException("timeoutMillis must be greater than or equal to 0, got " + timeoutMillis);
        }

        ProducerException firstException = null;
        LOGGER.info("Closing the log producer, timeoutMillis={}ms", timeoutMillis);

        long logProcessorCost = 0;
        try {
            logProcessorCost = logProcessor.close(timeoutMillis);
        } catch (ProducerException e) {
            firstException = e;
        }
        timeoutMillis = Math.max(0, timeoutMillis - logProcessorCost);
        LOGGER.debug("After close logProcessor, cost={}ms, remaining timeoutMillis={}ms", logProcessorCost, timeoutMillis);

        long retryQueueCost = 0;
        try {
            retryQueueCost = retryQueue.close(timeoutMillis);
        } catch (ProducerException e) {
            if (firstException == null) {
                firstException = e;
            }
        }
        timeoutMillis = Math.max(0, timeoutMillis - retryQueueCost);
        LOGGER.debug("After close retryQueue, cost={}ms, remaining timeoutMillis={}ms", retryQueueCost,
                timeoutMillis);

        long retryHandlerCost = 0;
        try {
            retryHandlerCost = retryHandler.close(timeoutMillis);
        } catch (ProducerException e) {
            if (firstException == null) {
                firstException = e;
            }
        }
        timeoutMillis = Math.max(0, timeoutMillis - retryHandlerCost);
        LOGGER.debug("After close retryHandler, cost={}ms, remaining timeoutMillis={}ms", retryHandlerCost,
                timeoutMillis);

        long batchSenderCost = 0;
        try {
            batchSenderCost = batchSender.close(timeoutMillis);
        } catch (ProducerException e) {
            if (firstException == null) {
                firstException = e;
            }
        }
        timeoutMillis = Math.max(0, timeoutMillis - batchSenderCost);
        LOGGER.debug("After close batchSender, cost={}ms, remaining timeoutMillis={}ms", batchSenderCost,
                timeoutMillis);

        long httpExecutorCost = 0;
        try {
            httpExecutorCost = closeHttpExecutor(timeoutMillis);
        } catch (ProducerException e) {
            if (firstException == null) {
                firstException = e;
            }
        }
        timeoutMillis = Math.max(0, timeoutMillis - httpExecutorCost);
        LOGGER.debug("After close httpExecutor, cost={}ms, remaining timeoutMillis={}ms", httpExecutorCost,
                timeoutMillis);

        long successHandlerCost = 0;
        try {
            successHandlerCost = successHandler.close(timeoutMillis);
        } catch (ProducerException e) {
            if (firstException == null) {
                firstException = e;
            }
        }
        timeoutMillis = Math.max(0, timeoutMillis - successHandlerCost);
        LOGGER.debug("After close successHandler, cost={}ms, remaining timeoutMillis={}ms", successHandlerCost,
                timeoutMillis);

        long failureHandlerCost = 0;
        try {
            failureHandlerCost = failureHandler.close(timeoutMillis);
        } catch (ProducerException e) {
            if (firstException == null) {
                firstException = e;
            }
        }
        timeoutMillis = Math.max(0, timeoutMillis - failureHandlerCost);
        LOGGER.debug("After close failureHandler, cost={}ms, remaining timeoutMillis={}ms", failureHandlerCost,
                timeoutMillis);

        if (firstException != null) {
            throw firstException;
        }
        LOGGER.info("The log producer has been closed");
    }

    private long closeHttpExecutor(long timeoutMillis) throws InterruptedException, ProducerException {
        long startMills = System.currentTimeMillis();
        httpExecutor.shutdown();
        if (httpExecutor.awaitTermination(timeoutMillis, TimeUnit.MILLISECONDS)) {
            LOGGER.debug("The httpExecutor is terminated");
        } else {
            LOGGER.warn("The httpExecutor is not fully terminated");
            throw new ProducerException("the httpExecutor is not fully terminated");
        }
        long nowMillis = System.currentTimeMillis();
        return nowMillis - startMills;
    }

    @Override
    public ProducerConfig getProducerConfig() {
        return producerConfig;
    }

    @Override
    public int availableMemoryInBytes() {
        return resourceHolder.getMemoryController().availablePermits();
    }

    @Override
    public int getLogCount() {
        return resourceHolder.getLogCount().get();
    }

    @Override
    public void putRegionConfig(RegionConfig regionConfig) {
        LogClient logClient = buildLogClient(regionConfig);
        this.logClientPool.put(regionConfig.getRegionId(), logClient);
    }

    @Override
    public void removeRegionConfig(RegionConfig regionConfig) {
        this.logClientPool.remove(regionConfig.getRegionId());
    }

    private LogClient buildLogClient(RegionConfig regionConfig) {
        ClientConfig clientConfig = new ClientConfig(regionConfig.getAccessKeyId(),
                regionConfig.getSecretAccessKey(), regionConfig.getRegionId(), regionConfig.getEndpoint());
        return new LogClient(clientConfig, httpExecutor);
    }
}
