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

        resourceHolder.acquire(logItems.size(), sizeInBytes, producerConfig.getMaxBlockMillis());

        try {
            GroupKey groupKey = new GroupKey(regionId, logTopic, source, fileName);
            DisruptorHandler<LogEvent> disruptor = getOrCreateDisruptorHandler(groupKey);
            disruptor.publish(logEvent);
        } catch (Throwable e) {
            resourceHolder.release(logItems.size(), sizeInBytes);
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
