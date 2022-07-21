package com.jdcloud.logs.producer.core;

import com.jdcloud.logs.api.common.LogItem;
import com.jdcloud.logs.api.util.StringUtils;
import com.jdcloud.logs.producer.config.ProducerConfig;
import com.jdcloud.logs.producer.disruptor.DisruptorHandler;
import com.jdcloud.logs.producer.disruptor.LogEvent;
import com.jdcloud.logs.producer.disruptor.LogEventTranslator;
import com.jdcloud.logs.producer.errors.LogSizeTooLargeException;
import com.jdcloud.logs.producer.errors.ProducerException;
import com.jdcloud.logs.producer.util.LogSizeCalculator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
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

    private final Map<GroupKey, DisruptorHandler<LogEvent, GroupKey>> disruptorHandlers;

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
        this.disruptorHandlers = new ConcurrentHashMap<GroupKey, DisruptorHandler<LogEvent, GroupKey>>();
        this.producerName = producerName;
        this.closed = false;
    }

    public void process(String regionId, String logTopic, List<LogItem> logItems, String source, String fileName)
            throws InterruptedException, ProducerException {
        if (isClosed()) {
            throw new IllegalStateException("Cannot append after the log process handler was closed");
        }

        List<LogEvent> logEvents = convertEvent(logItems);
        int sizeInBytes = LogSizeCalculator.calculateAndCheck(logEvents, producerConfig.getBatchSizeInBytes());
        if (sizeInBytes > ProducerConfig.MAX_BATCH_SIZE_IN_BYTES) {
            throw new LogSizeTooLargeException("the logs is " + sizeInBytes
                    + " bytes which is larger than MAX_BATCH_SIZE_IN_BYTES " + ProducerConfig.MAX_BATCH_SIZE_IN_BYTES);
        }
        if (sizeInBytes > producerConfig.getTotalSizeInBytes()) {
            throw new LogSizeTooLargeException("the logs is " + sizeInBytes
                    + " bytes which is larger than the totalSizeInBytes " + producerConfig.getTotalSizeInBytes());
        }

        resourceHolder.acquire(logItems.size(), sizeInBytes, producerConfig.getMaxBlockMillis());

        try {
            GroupKey groupKey = new GroupKey(regionId, logTopic, source, fileName);
            DisruptorHandler<LogEvent, GroupKey> disruptor = getOrCreateDisruptorHandler(groupKey);
            disruptor.publish(logEvents);
        } catch (Throwable e) {
            resourceHolder.release(logItems.size(), sizeInBytes);
            throw new ProducerException(e);
        }
    }

    private List<LogEvent> convertEvent(List<LogItem> logItems) {
        List<LogEvent> logEvents = new LinkedList<LogEvent>();
        for (LogItem logItem : logItems) {
            LogEvent logEvent = new LogEvent();
            logEvent.setLogItem(logItem);
            logEvents.add(logEvent);
        }
        return logEvents;
    }

    private DisruptorHandler<LogEvent, GroupKey> getOrCreateDisruptorHandler(GroupKey groupKey) {
        DisruptorHandler<LogEvent, GroupKey> disruptorHandler = disruptorHandlers.get(groupKey);
        if (disruptorHandler != null) {
            return disruptorHandler;
        }

        synchronized (LOCKER) {
            disruptorHandler = disruptorHandlers.get(groupKey);
            if (disruptorHandler != null) {
                return disruptorHandler;
            }

            disruptorHandler = new DisruptorHandler<LogEvent, GroupKey>(LogEvent.FACTORY, producerName,
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
        for (DisruptorHandler<LogEvent, GroupKey> disruptor : disruptorHandlers.values()) {
            disruptor.close(timeoutMillis);
        }
    }

    public static class GroupKey {

        private static final String DELIMITER = "|";
        private static final String DEFAULT_VALUE = "_";
        private final String key;
        private final String regionId;
        private final String logTopic;
        private final String source;
        private final String fileName;

        public GroupKey(String regionId, String logTopic, String source, String fileName) {
            this.regionId = regionId;
            this.logTopic = logTopic;
            this.source = source;
            this.fileName = fileName;
            this.key = getOrDefault(regionId) + DELIMITER
                    + getOrDefault(logTopic) + DELIMITER
                    + getOrDefault(source) + DELIMITER
                    + getOrDefault(fileName);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            GroupKey groupKey = (GroupKey) o;

            return key.equals(groupKey.key);
        }

        @Override
        public int hashCode() {
            return key.hashCode();
        }

        @Override
        public String toString() {
            return key;
        }

        public String getKey() {
            return key;
        }

        public String getRegionId() {
            return regionId;
        }

        public String getLogTopic() {
            return logTopic;
        }

        public String getSource() {
            return source;
        }

        public String getFileName() {
            return fileName;
        }

        private String getOrDefault(String value) {
            return StringUtils.defaultIfBlank(value, DEFAULT_VALUE);
        }
    }
}
