package com.jdcloud.logs.producer.core;

import com.jdcloud.logs.producer.errors.ProducerException;

/**
 * 关闭
 *
 * @author liubai
 * @date 2022/7/7
 */
public abstract class AbstractCloser implements Closeable {

    @Override
    public long close(long timeoutMillis) throws ProducerException, InterruptedException {
        long startMills = System.currentTimeMillis();
        doClose(timeoutMillis);
        long nowMillis = System.currentTimeMillis();
        return nowMillis - startMills;
    }

    public abstract void doClose(long timeoutMillis) throws InterruptedException, ProducerException;
}
