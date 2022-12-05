package com.jdcloud.logs.producer.disruptor;

import com.google.common.util.concurrent.SettableFuture;
import com.jdcloud.logs.api.common.LogItem;
import com.jdcloud.logs.producer.res.Response;

import java.util.List;

/**
 * event
 *
 * @author liubai
 * @date 2022/7/10
 */
public interface Event {

    List<LogItem> getLogItems();

    int getSizeInBytes();

    int getLogCount();

    SettableFuture<Response> getFuture();
}
