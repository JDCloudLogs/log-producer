package com.jdcloud.logs.producer.util;

import com.jdcloud.logs.api.common.LogContent;
import com.jdcloud.logs.api.common.LogItem;
import com.jdcloud.logs.producer.disruptor.Event;
import com.jdcloud.logs.producer.errors.LogSizeTooLargeException;

import java.util.List;

public class LogSizeCalculator {

    public static <T extends Event> int calculate(List<T> logEvents) {
        int sizeInBytes = 0;
        for (T logEvent : logEvents) {
            sizeInBytes += logEvent.getSizeInBytes();
        }
        return sizeInBytes;
    }

    public static <T extends LogItem> int calculateAndCheck(List<T> logItems, int batchSizeInBytes)
            throws LogSizeTooLargeException {
        int sizeInBytes = 0;
        for (T logItem : logItems) {
            int singleSizeInBytes = calculate(logItem);
            if (singleSizeInBytes > batchSizeInBytes) {
                throw new LogSizeTooLargeException("the log is " + singleSizeInBytes
                        + " bytes which is larger than the batchSizeInBytes configured");
            }
            sizeInBytes += singleSizeInBytes;
        }
        return sizeInBytes;
    }

    public static int calculate(LogItem logItem)
            throws LogSizeTooLargeException {
        if (logItem == null) {
            return 0;
        }
        int sizeInBytes = 8;
        for (LogContent content : logItem.getContents()) {
            if (content.getKey() != null) {
                sizeInBytes += content.getKey().length();
            }
            if (content.getValue() != null) {
                sizeInBytes += content.getValue().length();
            }
        }
        return sizeInBytes;
    }
}
