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
        logEvent.setLogItem(logItem.getLogItem());
        logEvent.setSizeInBytes(logItem.getSizeInBytes());
    }
}