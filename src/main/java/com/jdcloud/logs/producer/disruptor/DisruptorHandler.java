package com.jdcloud.logs.producer.disruptor;

import com.jdcloud.logs.producer.core.AbstractCloser;
import com.jdcloud.logs.producer.core.ResourceHolder;
import com.jdcloud.logs.producer.core.Sender;
import com.jdcloud.logs.producer.errors.ProducerException;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventTranslatorOneArg;

import java.util.List;

public class DisruptorHandler<E extends LogSizeCalculable, P> extends AbstractCloser {

    private final DisruptorRunner<E> disruptorRunner;
    private final LogEventHandler<E, P> logEventHandler;
    private EventTranslatorOneArg<E, E> translator;
    private volatile boolean closed;

    public DisruptorHandler(EventFactory eventFactory, String threadNamePrefix, int batchSize, int batchSizeInBytes,
                            int batchMillis, P params, Sender<E, P> sender, ResourceHolder resourceHolder) {
        logEventHandler = new LogEventHandler<E, P>(threadNamePrefix, batchSize, batchSizeInBytes, batchMillis,
                sender, params, resourceHolder);
        disruptorRunner = DisruptorRunner.newBuilder()
                .withEventFactory(eventFactory)
                .withEventHandler(logEventHandler)
                .withThreadFactoryNamePrefix(threadNamePrefix)
                .build();
    }

    public void publish(List<E> logs) {
        if (isClosed()) {
            throw new IllegalStateException("Cannot append after the disruptor handler was closed");
        }
        for (E log : logs) {
            disruptorRunner.publishEvent(translator, log);
        }
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
