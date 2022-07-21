package com.jdcloud.logs.producer.core;

import com.jdcloud.logs.api.common.LogItem;
import com.jdcloud.logs.producer.util.LogSizeCalculator;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * log batch
 *
 * @author liubai
 * @date 2022/6/30
 */
public class LogBatch implements Delayed {

    private final List<LogItem> logItems;
    private final LogProcessor.GroupKey groupKey;
    private long nextRetryMillis;
    private int retries = 0;
    private final int batchSizeInBytes;
    private final int batchCount;

    public LogBatch(List<LogItem> logItems, LogProcessor.GroupKey groupKey, int batchSizeInBytes) {
        this.logItems = logItems;
        this.groupKey = groupKey;
        this.batchSizeInBytes = batchSizeInBytes;
        this.batchCount = logItems.size();
    }

    public void increaseRetries() {
        this.retries++;
    }

    @Override
    public long getDelay(@Nonnull TimeUnit unit) {
        return unit.convert(nextRetryMillis - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(@Nonnull Delayed o) {
        return (int) (nextRetryMillis - ((LogBatch) o).getNextRetryMillis());
    }

    public int getRetries() {
        return this.retries;
    }

    public List<LogItem> getLogItems() {
        return logItems;
    }

    public LogProcessor.GroupKey getGroupKey() {
        return groupKey;
    }

    public String getRegionId() {
        return getGroupKey().getRegionId();
    }

    public String getLogTopic() {
        return getGroupKey().getLogTopic();
    }

    public String getSource() {
        return getGroupKey().getSource();
    }

    public String getFileName() {
        return getGroupKey().getFileName();
    }

    public long getNextRetryMillis() {
        return nextRetryMillis;
    }

    public void setNextRetryMillis(long nextRetryMillis) {
        this.nextRetryMillis = nextRetryMillis;
    }

    public int getBatchSizeInBytes() {
        return batchSizeInBytes;
    }

    public int getBatchCount() {
        return batchCount;
    }

    @Override
    public String toString() {
        return "LogBatch{" +
                "groupKey=" + groupKey +
                ", nextRetryMillis=" + nextRetryMillis +
                ", retries=" + retries +
                ", batchSizeInBytes=" + batchSizeInBytes +
                ", batchCount=" + batchCount +
                '}';
    }
}