package com.jdcloud.logs.producer.core;

import com.jdcloud.logs.producer.errors.ProducerException;
import com.jdcloud.logs.producer.errors.TimeoutException;
import com.jdcloud.logs.producer.util.LogUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 资源管理者
 *
 * @author liubai
 * @date 2022/7/13
 */
public class ResourceHolder {

    private static final Logger LOGGER = LoggerFactory.getLogger(ResourceHolder.class);

    private final Semaphore memoryController;

    private final AtomicInteger logCount = new AtomicInteger(0);

    public ResourceHolder(int totalSizeInBytes) {
        this.memoryController = new Semaphore(totalSizeInBytes);
    }

    public void acquire(int size, int sizeInBytes, long maxBlockMillis) throws InterruptedException, ProducerException {
        LogUtils.message(Level.TRACE, LOGGER, true,
                "Ready to acquire resource, logCount={}, availableSizeInBytes={}", logCount,
                memoryController.availablePermits());

        if (maxBlockMillis > 0) {
            boolean acquired = memoryController.tryAcquire(sizeInBytes, maxBlockMillis, TimeUnit.MILLISECONDS);
            if (!acquired) {
                LOGGER.warn("Failed to acquire memory within the configured max blocking time {} ms, "
                                + "requiredSizeInBytes={}, availableSizeInBytes={}",
                        maxBlockMillis, sizeInBytes, memoryController.availablePermits());
                throw new TimeoutException("failed to acquire memory within the configured max blocking time "
                        + maxBlockMillis + " ms");
            }
        } else {
            memoryController.acquire(sizeInBytes);
        }

        logCount.addAndGet(size);
    }

    public void release(int size, int sizeInBytes) {
        LOGGER.trace("Ready to release, log size:{}, bytes:{}", size, sizeInBytes);
        logCount.addAndGet(-size);
        memoryController.release(sizeInBytes);
    }

    public Semaphore getMemoryController() {
        return memoryController;
    }

    public AtomicInteger getLogCount() {
        return logCount;
    }
}
