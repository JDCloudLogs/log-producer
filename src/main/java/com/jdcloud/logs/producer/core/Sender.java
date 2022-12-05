package com.jdcloud.logs.producer.core;

import com.jdcloud.logs.producer.disruptor.Event;

import java.util.List;

/**
 * sender
 *
 * @author liubai
 * @date 2022/7/12
 */
public interface Sender<E extends Event> {
    void send(LogBatch logBatch);
}
