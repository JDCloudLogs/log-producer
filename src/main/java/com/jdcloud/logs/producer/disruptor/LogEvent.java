package com.jdcloud.logs.producer.disruptor;

import com.google.common.util.concurrent.SettableFuture;
import com.jdcloud.logs.api.common.LogItem;
import com.jdcloud.logs.producer.res.Response;
import com.lmax.disruptor.EventFactory;

import java.util.List;

/**
 * 日志对象
 *
 * @author liubai
 * @date 2022/7/10
 */
public class LogEvent implements Event {

    public static final Factory FACTORY = new Factory();

    private List<LogItem> logItems;
    private int sizeInBytes;
    private int logCount;
    private SettableFuture<Response> future;

    private static class Factory implements EventFactory<LogEvent> {
        @Override
        public LogEvent newInstance() {
            return new LogEvent();
        }
    }

    @Override
    public List<LogItem> getLogItems() {
        return logItems;
    }

    public void setLogItems(List<LogItem> logItems) {
        this.logItems = logItems;
    }

    @Override
    public int getLogCount() {
        return logCount;
    }

    public void setLogCount(int logCount) {
        this.logCount = logCount;
    }

    @Override
    public int getSizeInBytes() {
        return this.sizeInBytes;
    }

    public void setSizeInBytes(int sizeInBytes) {
        this.sizeInBytes = sizeInBytes;
    }

    @Override
    public SettableFuture<Response> getFuture() {
        return future;
    }

    public void setFuture(SettableFuture<Response> future) {
        this.future = future;
    }
}
