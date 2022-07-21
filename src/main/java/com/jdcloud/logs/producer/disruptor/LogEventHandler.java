package com.jdcloud.logs.producer.disruptor;

import com.jdcloud.logs.producer.core.AbstractCloser;
import com.jdcloud.logs.producer.core.ResourceHolder;
import com.jdcloud.logs.producer.core.Sender;
import com.jdcloud.logs.producer.errors.ProducerException;
import com.jdcloud.logs.producer.util.LogSizeCalculator;
import com.jdcloud.logs.producer.util.LogThreadFactory;
import com.lmax.disruptor.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
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
public class LogEventHandler<E extends LogSizeCalculable, P> extends AbstractCloser implements EventHandler<E> {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogEventHandler.class);

    private static final String BATCH_EXECUTOR_PREFIX = "-disruptor-event-handler-";
    private List<E> list = new LinkedList<E>();
    private final AtomicReference<Long> clockRef = new AtomicReference<Long>(System.currentTimeMillis());
    private final ScheduledExecutorService executor;
    private final Sender<E, P> sender;
    private final P params;
    private final int batchSize;
    private final int batchSizeInBytes;
    private final ResourceHolder resourceHolder;

    public LogEventHandler(String threadNamePrefix, int batchSize, int batchSizeInBytes, final int batchMillis,
                           Sender<E, P> sender, P params, ResourceHolder resourceHolder) {
        this.batchSize = batchSize;
        this.batchSizeInBytes = batchSizeInBytes;
        this.sender = sender;
        this.params = params;
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
        if (!tryAppend(list, event)) {
            send();
        }
        list.add(event);
    }

    private boolean tryAppend(List<E> list, E event) {
        boolean sizeMeet = list.size() + 1 <= batchSize;
        boolean bytesMeet = LogSizeCalculator.calculate(list) + event.getSizeInBytes() <= batchSizeInBytes;
        return sizeMeet && bytesMeet;
    }

    private void send() {
        try {
            Long clock = clockRef.get();
            if (clockRef.compareAndSet(clock, System.currentTimeMillis())) {
                List<E> sendList = list;
                list = new LinkedList<E>();
                sender.send(sendList, params);
            }
        } catch (Throwable e) {
            LOGGER.error("Send batch log error: {}", e.getMessage());
            if (LOGGER.isDebugEnabled()) {
                LOGGER.error(e.getMessage(), e);
            }
            resourceHolder.release(list.size(), LogSizeCalculator.calculate(list));
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
