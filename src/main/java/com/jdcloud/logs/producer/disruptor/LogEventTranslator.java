package com.jdcloud.logs.producer.disruptor;

import com.lmax.disruptor.EventTranslatorOneArg;

/**
 * disruptor translator
 *
 * @author liubai
 * @date 2022/7/12
 */
public class LogEventTranslator implements EventTranslatorOneArg<LogEvent, LogEvent> {

    @Override
    public void translateTo(LogEvent logEvent, long sequence, LogEvent logItem) {
        logEvent.setLogItems(logItem.getLogItems());
        logEvent.setSizeInBytes(logItem.getSizeInBytes());
        logEvent.setLogCount(logItem.getLogCount());
        logEvent.setFuture(logItem.getFuture());
    }
}