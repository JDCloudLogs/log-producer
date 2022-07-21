package com.jdcloud.logs.producer.disruptor;

import com.jdcloud.logs.producer.core.AbstractCloser;
import com.jdcloud.logs.producer.util.LogThreadFactory;
import com.jdcloud.logs.producer.util.LogUtils;
import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.util.concurrent.TimeUnit;

/**
 * @param <E>
 * @author zhangchen
 */
public class DisruptorRunner<E> extends AbstractCloser {

    private static final Logger LOGGER = LoggerFactory.getLogger(DisruptorRunner.class);

    private static final int SLEEP_MILLIS_BETWEEN_DRAIN_ATTEMPTS = 50;
    private static final int MAX_DRAIN_ATTEMPTS_BEFORE_SHUTDOWN = 200;

    private static final String DISRUPTOR_THREAD_PREFIX = "-disruptor-";

    private int bufferSize = 1 << 13;

    private Disruptor<E> disruptor;

    private RingBuffer<E> ringBuffer;

    private EventFactory<E> eventFactory;

    private EventHandler<E> eventHandler;

    private String threadFactoryNamePrefix;

    private volatile boolean closed;

    protected DisruptorRunner<E> init() {
        disruptor = new Disruptor<E>(eventFactory, bufferSize,
                new LogThreadFactory(threadFactoryNamePrefix + DISRUPTOR_THREAD_PREFIX),
                ProducerType.MULTI, new BlockingWaitStrategy());
        disruptor.handleEventsWith(eventHandler);
        disruptor.setDefaultExceptionHandler(new IgnoreExceptionHandler());
        this.ringBuffer = disruptor.start();
        return this;
    }

    public void publishEvent(EventTranslatorOneArg<E, E> translator, E log) {
        LogUtils.message(Level.TRACE, LOGGER, true,
                "Ready to publish event to ringBuffer, ringBuffer size={}, remaining capacity={}",
                ringBuffer.getBufferSize(), ringBuffer.remainingCapacity());

        if (isClosed()) {
            throw new IllegalStateException("Cannot publish event after the disruptor was closed");
        }
        ringBuffer.publishEvent(translator, log);
    }

    public static <T> DisruptorRunnerBuilder<T> newBuilder() {
        return new DisruptorRunnerBuilder<T>();
    }

    public static class DisruptorRunnerBuilder<T> {
        DisruptorRunner<T> runner = new DisruptorRunner<T>();

        public DisruptorRunnerBuilder<T> withEventFactory(EventFactory<T> eventFactory) {
            runner.setEventFactory(eventFactory);
            return this;
        }

        public DisruptorRunnerBuilder<T> withEventHandler(EventHandler<T> handler) {
            runner.setEventHandler(handler);
            return this;
        }

        public DisruptorRunnerBuilder<T> setBufferSize(int buffer) {
            runner.setBufferSize(buffer);
            return this;
        }

        public DisruptorRunnerBuilder<T> withThreadFactoryNamePrefix(String threadFactoryNamePrefix) {
            runner.setThreadFactoryNamePrefix(threadFactoryNamePrefix);
            return this;
        }

        public DisruptorRunner<T> build() {
            return runner.init();
        }
    }

    public boolean isClosed() {
        return closed;
    }

    @Override
    public void doClose(long timeoutMillis) {
        this.closed = true;
        // Calling Disruptor.shutdown() will wait until all enqueued events are fully processed,
        // but this waiting happens in a busy-spin. To avoid (postpone) wasting CPU,
        // we sleep in short chunks, up to 10 seconds, waiting for the ringbuffer to drain.
        for (int i = 0; hasBacklog(disruptor) && i < MAX_DRAIN_ATTEMPTS_BEFORE_SHUTDOWN; i++) {
            try {
                // give up the CPU for a while
                Thread.sleep(SLEEP_MILLIS_BETWEEN_DRAIN_ATTEMPTS);
            } catch (final InterruptedException e) {
                // ignored
            }
        }
        try {
            // 防止shutdown的timeout时间溢出
            timeoutMillis = timeoutMillis > Integer.MAX_VALUE ? Integer.MAX_VALUE : timeoutMillis;
            // busy-spins until all events currently in the disruptor have been processed, or timeout
            disruptor.shutdown(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (final TimeoutException e) {
            // give up on remaining log events, if any
            LOGGER.trace("Discard remaining log events, halt disruptor, remaining size={}",
                    ringBuffer.remainingCapacity());
            disruptor.halt();
        }
    }

    private static boolean hasBacklog(final Disruptor<?> theDisruptor) {
        final RingBuffer<?> ringBuffer = theDisruptor.getRingBuffer();
        return !ringBuffer.hasAvailableCapacity(ringBuffer.getBufferSize());
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public void setEventFactory(EventFactory<E> eventFactory) {
        this.eventFactory = eventFactory;
    }

    public void setEventHandler(EventHandler<E> eventHandler) {
        this.eventHandler = eventHandler;
    }

    public void setThreadFactoryNamePrefix(String threadFactoryNamePrefix) {
        this.threadFactoryNamePrefix = threadFactoryNamePrefix;
    }
}
