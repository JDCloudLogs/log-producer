package com.jdcloud.logs.producer.core;

import com.google.common.util.concurrent.SettableFuture;
import com.jdcloud.logs.api.common.LogItem;
import com.jdcloud.logs.producer.config.ProducerConfig;
import com.jdcloud.logs.producer.disruptor.DisruptorHandler;
import com.jdcloud.logs.producer.disruptor.LogEvent;
import com.jdcloud.logs.producer.disruptor.LogEventTranslator;
import com.jdcloud.logs.producer.errors.LogSizeTooLargeException;
import com.jdcloud.logs.producer.errors.ProducerException;
import com.jdcloud.logs.producer.res.Response;
import com.jdcloud.logs.producer.util.LogSizeCalculator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 核心处理类
 *
 * @author liubai
 * @date 2022/6/29
 */
public class LogProcessor extends AbstractCloser {

    private static final Logger LOGGER = LoggerFactory.getLogger(BatchSender.class);

    private final Map<GroupKey, DisruptorHandler<LogEvent>> disruptorHandlers;

    private final Object[] LOCKER = new Object[0];

    private final BatchSender batchSender;

    private final ProducerConfig producerConfig;

    private final ResourceHolder resourceHolder;

    private final String producerName;

    private volatile boolean closed;

    public LogProcessor(BatchSender batchSender, ProducerConfig producerConfig, ResourceHolder resourceHolder,
                        String producerName) {
        this.batchSender = batchSender;
        this.producerConfig = producerConfig;
        this.resourceHolder = resourceHolder;
        this.disruptorHandlers = new ConcurrentHashMap<GroupKey, DisruptorHandler<LogEvent>>();
        this.producerName = producerName;
        this.closed = false;
    }

    public SettableFuture<Response> process(String regionId, String logTopic, List<LogItem> logItems, String source, String fileName)
            throws InterruptedException, ProducerException {
        if (isClosed()) {
            throw new IllegalStateException("Cannot append after the log process handler was closed");
        }

        int sizeInBytes = LogSizeCalculator.calculateAndCheck(logItems, producerConfig.getBatchSizeInBytes());
        if (sizeInBytes > ProducerConfig.MAX_BATCH_SIZE_IN_BYTES) {
            throw new LogSizeTooLargeException("the logs is " + sizeInBytes
                    + " bytes which is larger than MAX_BATCH_SIZE_IN_BYTES " + ProducerConfig.MAX_BATCH_SIZE_IN_BYTES);
        }
        if (sizeInBytes > producerConfig.getTotalSizeInBytes()) {
            throw new LogSizeTooLargeException("the logs is " + sizeInBytes
                    + " bytes which is larger than the totalSizeInBytes " + producerConfig.getTotalSizeInBytes());
        }

        LogEvent logEvent = buildEvent(logItems, sizeInBytes);

        // 尝试获取资源许可
        boolean resourceAcquired = false;
        boolean wasInterrupted = false;

        try {
            resourceHolder.acquire(logItems.size(), sizeInBytes, producerConfig.getMaxBlockMillis());
            resourceAcquired = true;
        } catch (InterruptedException e) {
            wasInterrupted = true;
            LOGGER.warn("Log processing interrupted during resource acquisition, logCount={}, sizeInBytes={}",
                    logItems.size(), sizeInBytes);

            // 检查是否配置了忽略中断继续发送
            if (producerConfig.isIgnoreInterruptOnSend()) {
                LOGGER.info("Attempting to send logs despite thread interruption (ignoreInterruptOnSend=true), " +
                        "logCount={}, sizeInBytes={}", logItems.size(), sizeInBytes);

                // 使用非阻塞方式尝试获取资源
                resourceAcquired = resourceHolder.tryAcquire(
                        logItems.size(),
                        sizeInBytes,
                        producerConfig.getInterruptSendTimeoutMillis()
                );

                if (!resourceAcquired) {
                    // 非阻塞获取失败，但日志很重要，尝试直接发布到 Disruptor（不占用资源配额）
                    // 这种情况下可能会导致内存超限，但确保日志不丢失
                    LOGGER.warn("Failed to acquire resource in non-blocking mode, publishing directly to ensure " +
                            "log delivery, logCount={}, sizeInBytes={}", logItems.size(), sizeInBytes);
                }
            } else {
                // 不忽略中断，恢复中断状态并抛出异常，此处日志保留策略为丢弃，需要需要根据发送方配置策略进行调整
                LOGGER.error("Log processing interrupted during resource acquisition, logCount={}, sizeInBytes={}",
                        logItems.size(), sizeInBytes);
                Thread.currentThread().interrupt();
                throw new ProducerException("Log processing was interrupted during resource acquisition", e);
            }
        }

        try {
            GroupKey groupKey = new GroupKey(regionId, logTopic, source, fileName);
            DisruptorHandler<LogEvent> disruptor = getOrCreateDisruptorHandler(groupKey);
            disruptor.publish(logEvent);
        } catch (Throwable e) {
            if (resourceAcquired) {
                resourceHolder.release(logItems.size(), sizeInBytes);
            }
            throw new ProducerException(e);
        }
        return logEvent.getFuture();
    }

    private LogEvent buildEvent(List<LogItem> logItems, int sizeInBytes) {
        LogEvent logEvent = new LogEvent();
        logEvent.setSizeInBytes(sizeInBytes);
        logEvent.setLogCount(logItems.size());
        logEvent.setLogItems(logItems);
        SettableFuture<Response> future = SettableFuture.create();
        logEvent.setFuture(future);
        return logEvent;
    }

    private DisruptorHandler<LogEvent> getOrCreateDisruptorHandler(GroupKey groupKey) {
        DisruptorHandler<LogEvent> disruptorHandler = disruptorHandlers.get(groupKey);
        if (disruptorHandler != null) {
            return disruptorHandler;
        }

        synchronized (LOCKER) {
            disruptorHandler = disruptorHandlers.get(groupKey);
            if (disruptorHandler != null) {
                return disruptorHandler;
            }

            disruptorHandler = new DisruptorHandler<LogEvent>(LogEvent.FACTORY, producerName,
                    producerConfig.getBatchSize(), producerConfig.getBatchSizeInBytes(),
                    producerConfig.getBatchMillis(), groupKey, batchSender, resourceHolder);
            disruptorHandler.setTranslator(new LogEventTranslator());
            disruptorHandlers.put(groupKey, disruptorHandler);
            LOGGER.debug("Create a new disruptorHandler for groupKey:{}", groupKey);
            return disruptorHandler;
        }
    }

    public boolean isClosed() {
        return closed;
    }

    @Override
    public void doClose(long timeoutMillis) throws ProducerException, InterruptedException {
        this.closed = true;
        closeDisruptorHandlers(timeoutMillis);
    }

    private void closeDisruptorHandlers(long timeoutMillis) throws ProducerException, InterruptedException {
        for (DisruptorHandler<LogEvent> disruptor : disruptorHandlers.values()) {
            disruptor.close(timeoutMillis);
        }
    }
}
