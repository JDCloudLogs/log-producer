package com.jdcloud.logs.producer.disruptor;

import com.jdcloud.logs.producer.core.*;
import com.jdcloud.logs.producer.errors.ProducerException;
import com.jdcloud.logs.producer.util.LogThreadFactory;
import com.lmax.disruptor.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * log event handler
 *
 * @author liubai
 * @date 2022/7/12
 */
public class LogEventHandler<E extends Event> extends AbstractCloser implements EventHandler<E> {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogEventHandler.class);

    private static final String BATCH_EXECUTOR_PREFIX = "-disruptor-event-handler-";
    private final AtomicReference<Long> clockRef = new AtomicReference<Long>(System.currentTimeMillis());
    private final ScheduledExecutorService executor;
    private final Sender<E> sender;
    private final GroupKey groupKey;
    private final int batchSize;
    private final int batchSizeInBytes;
    private final ResourceHolder resourceHolder;
    private LogBatch logBatch;

    public LogEventHandler(String threadNamePrefix, int batchSize, int batchSizeInBytes, final int batchMillis,
                           Sender<E> sender, GroupKey groupKey, ResourceHolder resourceHolder) {
        this.batchSize = batchSize;
        this.batchSizeInBytes = batchSizeInBytes;
        this.sender = sender;
        this.groupKey = groupKey;
        this.logBatch = new LogBatch(groupKey);
        executor = new ScheduledThreadPoolExecutor(1,
                new LogThreadFactory(threadNamePrefix + BATCH_EXECUTOR_PREFIX));
        this.resourceHolder = resourceHolder;
        executor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                long now = System.currentTimeMillis();
                Long clock = clockRef.get();
                if (now >= clock + batchMillis) {
                    LogEventHandler.this.send();
                }
            }
        }, batchMillis, batchMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void onEvent(E event, long sequence, boolean endOfBatch) throws Exception {
        if (!tryAppend(event)) {
            send();
        }
        addEvent(event);
    }

    private void addEvent(E event) {
        logBatch.addLogItems(event.getLogItems(), event.getLogCount(), event.getSizeInBytes());
        logBatch.addFuture(event.getFuture());
    }

    private boolean tryAppend(E event) {
        boolean sizeMeet = logBatch.getBatchCount() + event.getLogCount() <= batchSize;
        boolean bytesMeet = logBatch.getBatchSizeInBytes() + event.getSizeInBytes() <= batchSizeInBytes;
        return sizeMeet && bytesMeet;
    }

    private void send() {
        try {
            Long clock = clockRef.get();
            if (clockRef.compareAndSet(clock, System.currentTimeMillis())) {
                LogBatch sendLogBatch = logBatch;
                logBatch = new LogBatch(groupKey);
                sender.send(sendLogBatch);
            }
        } catch (Throwable e) {
            LOGGER.error("Send batch log error: {}", e.getMessage());
            if (LOGGER.isDebugEnabled()) {
                LOGGER.error(e.getMessage(), e);
            }
            resourceHolder.release(logBatch.getBatchCount(), logBatch.getBatchSizeInBytes());
        }
    }

    private void submitBacklog() {
        send();
    }

    @Override
    public void doClose(long timeoutMillis) throws InterruptedException, ProducerException {
        executor.shutdown();
        submitBacklog();
    }
}
