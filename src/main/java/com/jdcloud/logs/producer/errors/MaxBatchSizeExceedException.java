package com.jdcloud.logs.producer.errors;

public class MaxBatchSizeExceedException extends ProducerException {

    public MaxBatchSizeExceedException() {
        super();
    }

    public MaxBatchSizeExceedException(String message, Throwable cause) {
        super(message, cause);
    }

    public MaxBatchSizeExceedException(String message) {
        super(message);
    }

    public MaxBatchSizeExceedException(Throwable cause) {
        super(cause);
    }
}
