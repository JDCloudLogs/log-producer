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
    
    /**
     * 用于检测日志递归调用的 ThreadLocal
     * 防止在日志输出过程中再次触发日志输出，造成无限循环
     */
    private static final ThreadLocal<Boolean> IN_LOG_OUTPUT = new ThreadLocal<Boolean>() {
        @Override
        protected Boolean initialValue() {
            return Boolean.FALSE;
        }
    };

    private final Semaphore memoryController;

    private final AtomicInteger logCount = new AtomicInteger(0);

    public ResourceHolder(int totalSizeInBytes) {
        this.memoryController = new Semaphore(totalSizeInBytes);
    }

    /**
     * 获取资源许可
     *
     * @param size           日志条数
     * @param sizeInBytes    日志字节数
     * @param maxBlockMillis 最大阻塞时间
     * @throws InterruptedException 当线程被中断时抛出
     * @throws ProducerException    当获取资源超时时抛出
     */
    public void acquire(int size, int sizeInBytes, long maxBlockMillis) throws InterruptedException, ProducerException {
        LogUtils.message(Level.TRACE, LOGGER, true,
                "Ready to acquire resource, logCount={}, availableSizeInBytes={}", logCount,
                memoryController.availablePermits());

        try {
            if (maxBlockMillis > 0) {
                boolean acquired = memoryController.tryAcquire(sizeInBytes, maxBlockMillis, TimeUnit.MILLISECONDS);
                if (!acquired) {
                    safeLogWarn("Failed to acquire memory within the configured max blocking time {} ms, "
                                    + "requiredSizeInBytes={}, availableSizeInBytes={}",
                            maxBlockMillis, sizeInBytes, memoryController.availablePermits());
                    throw new TimeoutException("failed to acquire memory within the configured max blocking time "
                            + maxBlockMillis + " ms");
                }
            } else {
                memoryController.acquire(sizeInBytes);
            }
        } catch (InterruptedException e) {
            safeLogWarn("Thread interrupted while acquiring memory resource, requiredSizeInBytes={}", sizeInBytes);
            Thread.currentThread().interrupt(); // 重新设置中断状态
            throw e;
        }

        logCount.addAndGet(size);
    }

    /**
     * 尝试获取资源许可（非阻塞或短时间阻塞）
     * 此方法用于线程中断时尝试尽可能发送日志，避免日志丢失
     *
     * @param size           日志条数
     * @param sizeInBytes    日志字节数
     * @param timeoutMillis  超时时间（毫秒），0表示不等待
     * @return true 如果成功获取资源，false 如果获取失败
     */
    public boolean tryAcquire(int size, int sizeInBytes, long timeoutMillis) {
        LogUtils.message(Level.TRACE, LOGGER, true,
                "Try to acquire resource (non-blocking), logCount={}, availableSizeInBytes={}, timeoutMillis={}",
                logCount, memoryController.availablePermits(), timeoutMillis);

        boolean acquired = false;
        try {
            if (timeoutMillis > 0) {
                acquired = memoryController.tryAcquire(sizeInBytes, timeoutMillis, TimeUnit.MILLISECONDS);
            } else {
                acquired = memoryController.tryAcquire(sizeInBytes);
            }
        } catch (InterruptedException e) {
            // 中断时再次尝试非阻塞获取
            safeLogWarn("Thread interrupted during tryAcquire, attempting non-blocking acquire, requiredSizeInBytes={}",
                    sizeInBytes);
            Thread.currentThread().interrupt();
            acquired = memoryController.tryAcquire(sizeInBytes);
        }

        if (acquired) {
            logCount.addAndGet(size);
            return true;
        }
        return false;
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
    
    /**
     * 安全的日志输出方法，防止递归调用
     * 使用 ThreadLocal 检测是否已经在日志输出过程中
     * 如果是，则直接输出到 System.err，避免触发 appender 导致递归
     */
    private void safeLogWarn(String format, Object... args) {
        // 检查是否已经在日志输出过程中
        if (IN_LOG_OUTPUT.get()) {
            // 已经在日志输出过程中，直接输出到 System.err 避免递归
            String message = format;
            if (args != null && args.length > 0) {
                try {
                    message = String.format(format.replace("{}", "%s"), args);
                } catch (Exception e) {
                    // 格式化失败，使用原始消息
                }
            }
            System.err.println("[WARN] " + ResourceHolder.class.getSimpleName() + " - " + message);
            return;
        }
        
        try {
            // 设置标志，表示正在输出日志
            IN_LOG_OUTPUT.set(Boolean.TRUE);
            LOGGER.warn(format, args);
        } finally {
            // 清除标志
            IN_LOG_OUTPUT.remove();
        }
    }
}
