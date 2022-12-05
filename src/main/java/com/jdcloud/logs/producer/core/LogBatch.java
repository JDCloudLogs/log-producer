package com.jdcloud.logs.producer.core;

import com.google.common.util.concurrent.SettableFuture;
import com.jdcloud.logs.api.common.LogItem;
import com.jdcloud.logs.producer.res.Attempt;
import com.jdcloud.logs.producer.res.Response;

import javax.annotation.Nonnull;
import java.util.ArrayList;
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

    private List<LogItem> logItems;
    private final GroupKey groupKey;
    private long nextRetryMillis;
    private int batchCount;
    private int batchSizeInBytes;
    private final List<SettableFuture<Response>> futures;
    private final List<Attempt> attempts;
    private int attemptCount;

    public LogBatch(GroupKey groupKey) {
        this.groupKey = groupKey;
        this.batchCount = 0;
        this.batchSizeInBytes = 0;
        this.attemptCount = 0;
        this.futures = new ArrayList<SettableFuture<Response>>();
        this.attempts = new ArrayList<Attempt>();
    }

    @Override
    public long getDelay(@Nonnull TimeUnit unit) {
        return unit.convert(nextRetryMillis - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(@Nonnull Delayed o) {
        return (int) (nextRetryMillis - ((LogBatch) o).getNextRetryMillis());
    }

    public void addLogItems(List<LogItem> logItems, int logCount, int sizeInBytes) {
        if (this.logItems == null) {
            this.logItems = new ArrayList<LogItem>();
        }
        this.logItems.addAll(logItems);
        this.batchCount += logCount;
        this.batchSizeInBytes += sizeInBytes;
    }

    public List<LogItem> getLogItems() {
        return logItems;
    }

    public GroupKey getGroupKey() {
        return groupKey;
    }

    public long getNextRetryMillis() {
        return nextRetryMillis;
    }

    public void setNextRetryMillis(long nextRetryMillis) {
        this.nextRetryMillis = nextRetryMillis;
    }

    public int getRetries() {
        return attemptCount - 1;
    }

    public int getBatchSizeInBytes() {
        return batchSizeInBytes;
    }

    public int getBatchCount() {
        return batchCount;
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

    public void addFuture(SettableFuture<Response> future) {
        this.futures.add(future);
    }

    public List<SettableFuture<Response>> getFutures() {
        return futures;
    }

    public List<Attempt> getAttempts() {
        return attempts;
    }

    public void addAttempt(Attempt attempt) {
        this.attempts.add(attempt);
        this.attemptCount++;
    }

    public int getAttemptCount() {
        return attemptCount;
    }

    @Override
    public String toString() {
        return "LogBatch{" +
                "groupKey=" + groupKey +
                ", nextRetryMillis=" + nextRetryMillis +
                ", attemptCount=" + attemptCount +
                ", batchSizeInBytes=" + batchSizeInBytes +
                ", batchCount=" + batchCount +
                '}';
    }
}