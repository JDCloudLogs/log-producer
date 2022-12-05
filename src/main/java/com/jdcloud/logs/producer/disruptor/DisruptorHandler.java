package com.jdcloud.logs.producer.disruptor;

import com.jdcloud.logs.producer.core.AbstractCloser;
import com.jdcloud.logs.producer.core.GroupKey;
import com.jdcloud.logs.producer.core.ResourceHolder;
import com.jdcloud.logs.producer.core.Sender;
import com.jdcloud.logs.producer.errors.ProducerException;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventTranslatorOneArg;

public class DisruptorHandler<E extends Event> extends AbstractCloser {

    private final DisruptorRunner<E> disruptorRunner;
    private final LogEventHandler<E> logEventHandler;
    private EventTranslatorOneArg<E, E> translator;
    private volatile boolean closed;

    public DisruptorHandler(EventFactory eventFactory, String threadNamePrefix, int batchSize, int batchSizeInBytes,
                            int batchMillis, GroupKey groupKey, Sender<E> sender, ResourceHolder resourceHolder) {
        logEventHandler = new LogEventHandler<E>(threadNamePrefix, batchSize, batchSizeInBytes, batchMillis,
                sender, groupKey, resourceHolder);
        disruptorRunner = DisruptorRunner.newBuilder()
                .withEventFactory(eventFactory)
                .withEventHandler(logEventHandler)
                .withThreadFactoryNamePrefix(threadNamePrefix)
                .build();
    }

    public void publish(E log) {
        if (isClosed()) {
            throw new IllegalStateException("Cannot append after the disruptor handler was closed");
        }
        disruptorRunner.publishEvent(translator, log);
    }

    public void setTranslator(EventTranslatorOneArg<E, E> translator) {
        this.translator = translator;
    }

    public boolean isClosed() {
        return closed;
    }

    @Override
    public void doClose(long timeoutMillis) throws InterruptedException, ProducerException {
        this.closed = true;
        disruptorRunner.close(timeoutMillis);
        logEventHandler.close(timeoutMillis);
    }
}
