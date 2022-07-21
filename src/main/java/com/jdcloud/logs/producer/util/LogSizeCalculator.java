package com.jdcloud.logs.producer.util;

import com.jdcloud.logs.producer.disruptor.LogSizeCalculable;
import com.jdcloud.logs.producer.errors.LogSizeTooLargeException;

import java.util.List;

public class LogSizeCalculator {

    public static <T extends LogSizeCalculable> int calculate(List<T> logEvents) {
        int sizeInBytes = 0;
        for (T logEvent : logEvents) {
            sizeInBytes += logEvent.getSizeInBytes();
        }
        return sizeInBytes;
    }

    public static <T extends LogSizeCalculable> int calculateAndCheck(List<T> logEvents, int batchSizeInBytes)
            throws LogSizeTooLargeException {
        int sizeInBytes = 0;
        for (T logEvent : logEvents) {
            int singleSizeInBytes = logEvent.getSizeInBytes();
            if (singleSizeInBytes > batchSizeInBytes) {
                throw new LogSizeTooLargeException("the log is " + singleSizeInBytes
                        + " bytes which is larger than the batchSizeInBytes configured");
            }
            sizeInBytes += singleSizeInBytes;
        }
        return sizeInBytes;
    }
}
