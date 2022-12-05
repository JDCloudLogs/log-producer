package com.jdcloud.logs.producer.res;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.SettableFuture;
import com.jdcloud.logs.producer.core.AbstractCloser;
import com.jdcloud.logs.producer.core.LogBatch;
import com.jdcloud.logs.producer.core.ResourceHolder;
import com.jdcloud.logs.producer.core.RetryHandler;
import com.jdcloud.logs.producer.errors.ProducerException;
import com.jdcloud.logs.producer.errors.SendFailureException;
import com.jdcloud.logs.producer.util.LogThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * 处理结果
 *
 * @author liubai
 * @date 2022/11/22
 */
public class ResponseHandler extends AbstractCloser {

    private static final Logger LOGGER = LoggerFactory.getLogger(ResponseHandler.class);

    private static final String RESPONSE_HANDLER_SUFFIX = "-response-handler";

    private final BlockingQueue<LogBatch> bockingQueue;
    private final LogThread resThread;
    private final ResourceHolder resourceHolder;
    private final String flag;
    private volatile boolean closed;

    public ResponseHandler(BlockingQueue<LogBatch> bockingQueue, String producerName, String flag,
                           ResourceHolder resourceHolder) {
        this.bockingQueue = bockingQueue;
        this.flag = flag;
        String threadName = producerName + "-" + flag + RESPONSE_HANDLER_SUFFIX;
        this.resThread = new LogThread(threadName, true) {
            @Override
            public void run() {
                handle();
            }
        };
        this.resourceHolder = resourceHolder;
        this.closed = false;
    }

    public void start() {
        resThread.start();
    }

    private void handle() {
        loopHandleResponse();
        submitBacklogResponse();
    }

    private void loopHandleResponse() {
        while (!closed) {
            try {
                LogBatch logBatch = bockingQueue.take();
                handleResponse(logBatch);
            } catch (InterruptedException e) {
                LOGGER.error("Interrupted in " + flag + " handler", e);
            }
        }
    }

    private void submitBacklogResponse() {
        List<LogBatch> remainingResponses = new LinkedList<LogBatch>();
        bockingQueue.drainTo(remainingResponses);
        for (LogBatch logBatch : remainingResponses) {
            handleResponse(logBatch);
        }
    }

    private void handleResponse(LogBatch logBatch) {
        try {
            Attempt last = Iterables.getLast(logBatch.getAttempts());
            Response response = new Response(last.isSuccessful(), logBatch.getAttempts(), logBatch.getAttemptCount());
            for (SettableFuture<Response> future : logBatch.getFutures()) {
                try {
                    if (response.isSuccessful()) {
                        future.set(response);
                    } else {
                        future.setException(new SendFailureException(response));
                    }
                } catch (Exception e) {
                    LOGGER.error("Filed to set " + flag + " future");
                }
            }
        } catch (Throwable t) {
            LOGGER.error("Filed to handler " + flag + " response");
        } finally {
            release(logBatch.getBatchCount(), logBatch.getBatchSizeInBytes());
        }
    }

    private void release(int batchCount, int batchSizeInBytes) {
        resourceHolder.release(batchCount, batchSizeInBytes);
    }

    @Override
    public void doClose(long timeoutMillis) throws InterruptedException, ProducerException {
        this.closed = true;
        // 打断队列自旋，防止队列为空时无法跳出while循环
        resThread.interrupt();
        resThread.join(timeoutMillis);
        if (resThread.isAlive()) {
            LOGGER.warn("The " + flag + " handler thread is still alive");
            throw new ProducerException("the " + flag + " handler thread is still alive");
        }
    }
}
