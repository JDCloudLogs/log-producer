package com.jdcloud.logs.producer.disruptor;

import com.jdcloud.logs.api.common.LogContent;
import com.jdcloud.logs.api.common.LogItem;
import com.lmax.disruptor.EventFactory;

/**
 * 日志对象
 *
 * @author liubai
 * @date 2022/7/10
 */
public class LogEvent implements LogSizeCalculable {

    public static final Factory FACTORY = new Factory();

    private LogItem logItem;
    private int sizeInBytes;

    private static class Factory implements EventFactory<LogEvent> {
        @Override
        public LogEvent newInstance() {
            return new LogEvent();
        }
    }

    public LogItem getLogItem() {
        return logItem;
    }

    public void setLogItem(LogItem logItem) {
        this.logItem = logItem;
    }

    @Override
    public int getSizeInBytes() {
        if (logItem == null) {
            return 0;
        }
        if (this.sizeInBytes == 0) {
            int sizeInBytes = 8;
            for (LogContent content : logItem.getContents()) {
                if (content.getKey() != null) {
                    sizeInBytes += content.getKey().length();
                }
                if (content.getValue() != null) {
                    sizeInBytes += content.getValue().length();
                }
            }
            this.sizeInBytes = sizeInBytes;
        }
        return this.sizeInBytes;
    }

    public void setSizeInBytes(int sizeInBytes) {
        this.sizeInBytes = sizeInBytes;
    }

}
