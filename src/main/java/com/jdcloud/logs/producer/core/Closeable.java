package com.jdcloud.logs.producer.core;

import com.jdcloud.logs.producer.errors.ProducerException;

/**
 * 关闭
 *
 * @author liubai
 * @date 2022/7/7
 */
public interface Closeable {

    long close(long timeoutMillis) throws ProducerException, InterruptedException ;
}
