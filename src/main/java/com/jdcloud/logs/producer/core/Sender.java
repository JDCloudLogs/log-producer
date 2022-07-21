package com.jdcloud.logs.producer.core;

import com.jdcloud.logs.producer.disruptor.LogSizeCalculable;

import java.util.List;

/**
 * sender
 *
 * @author liubai
 * @date 2022/7/12
 */
public interface Sender<E extends LogSizeCalculable, P> {
    void send(List<E> list, P p);
}
