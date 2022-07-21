package com.jdcloud.logs.producer.disruptor;

/**
 * 计算字节大小
 *
 * @author liubai
 * @date 2022/7/10
 */
public interface LogSizeCalculable {

    int getSizeInBytes();
}
