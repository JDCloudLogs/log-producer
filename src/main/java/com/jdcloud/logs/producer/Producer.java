package com.jdcloud.logs.producer;

import com.jdcloud.logs.api.common.LogItem;
import com.jdcloud.logs.producer.config.ProducerConfig;
import com.jdcloud.logs.producer.config.RegionConfig;
import com.jdcloud.logs.producer.errors.ProducerException;

import java.util.List;

/**
 * The interface for the {@link LogProducer}
 *
 * @see LogProducer
 */
public interface Producer {

    void send(String regionId, String logTopic, LogItem logItem)
            throws ProducerException, InterruptedException;

    void send(String regionId, String logTopic, LogItem logItem, String source, String fileName)
            throws ProducerException, InterruptedException;

    void send(String regionId, String logTopic, List<LogItem> logItems)
            throws ProducerException, InterruptedException;

    void send(String regionId, String logTopic, List<LogItem> logItems, String source, String fileName)
            throws ProducerException, InterruptedException;

    void close() throws InterruptedException, ProducerException;

    void close(long timeoutMillis) throws InterruptedException, ProducerException;

    ProducerConfig getProducerConfig();

    int availableMemoryInBytes();

    int getLogCount();

    void putRegionConfig(RegionConfig regionConfig);

    void removeRegionConfig(RegionConfig regionConfig);
}
