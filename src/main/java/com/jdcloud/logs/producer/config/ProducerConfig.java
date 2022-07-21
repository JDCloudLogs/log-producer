package com.jdcloud.logs.producer.config;

public class ProducerConfig {

    public static final int DEFAULT_TOTAL_SIZE_IN_BYTES = 100 * 1024 * 1024;

    public static final int DEFAULT_SEND_THREADS = Math.max(Runtime.getRuntime().availableProcessors(), 1);

    public static final int DEFAULT_BATCH_SIZE = 4096;

    public static final int DEFAULT_BATCH_SIZE_IN_BYTES = 2 * 1024 * 1024;

    public static final int MAX_BATCH_SIZE = 32768;

    public static final int MAX_BATCH_SIZE_IN_BYTES = 4 * 1024 * 1024;

    public static final int DEFAULT_BATCH_MILLIS = 2000;

    public static final int MIN_BATCH_MILLIS = 100;

    public static final int DEFAULT_RETRIES = 10;

    public static final long DEFAULT_INIT_RETRY_BACKOFF_MILLIS = 100L;

    public static final long DEFAULT_MAX_RETRY_BACKOFF_MILLIS = 50 * 1000L;

    private int totalSizeInBytes = DEFAULT_TOTAL_SIZE_IN_BYTES;

    private long maxBlockMillis;

    private int sendThreads = DEFAULT_SEND_THREADS;

    private int batchSize = DEFAULT_BATCH_SIZE;

    private int batchSizeInBytes = DEFAULT_BATCH_SIZE_IN_BYTES;

    private int batchMillis = DEFAULT_BATCH_MILLIS;

    private int retries = DEFAULT_RETRIES;

    private long initRetryBackoffMillis = DEFAULT_INIT_RETRY_BACKOFF_MILLIS;

    private long maxRetryBackoffMillis = DEFAULT_MAX_RETRY_BACKOFF_MILLIS;

    /**
     * 日志收敛开关，调试模式下有些频繁打印的日志可以做收敛
     */
    private boolean logConvergence = false;

    /**
     * 日志收敛时间，调试模式下频繁打印的日志的收敛时间
     */
    private int logConvergenceMillis = 30000;

    public int getTotalSizeInBytes() {
        return totalSizeInBytes;
    }

    public void setTotalSizeInBytes(int totalSizeInBytes) {
        if (totalSizeInBytes <= 0) {
            throw new IllegalArgumentException("totalSizeInBytes must be greater than 0, got " + totalSizeInBytes);
        }
        this.totalSizeInBytes = totalSizeInBytes;
    }

    public long getMaxBlockMillis() {
        return maxBlockMillis;
    }

    public void setMaxBlockMillis(long maxBlockMillis) {
        this.maxBlockMillis = maxBlockMillis;
    }

    public int getSendThreads() {
        return sendThreads;
    }

    public void setSendThreads(int sendThreads) {
        if (sendThreads <= 0) {
            throw new IllegalArgumentException("sendThreads must be greater than 0, got " + sendThreads);
        }
        this.sendThreads = sendThreads;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        if (batchSize < 1 || batchSize > MAX_BATCH_SIZE) {
            throw new IllegalArgumentException(String.format("batchSize must be between 1 and %d, got %d",
                    MAX_BATCH_SIZE, batchSize));
        }
        this.batchSize = batchSize;
    }

    public int getBatchSizeInBytes() {
        return batchSizeInBytes;
    }

    public void setBatchSizeInBytes(int batchSizeInBytes) {
        if (batchSizeInBytes < 1 || batchSizeInBytes > MAX_BATCH_SIZE_IN_BYTES) {
            throw new IllegalArgumentException(String.format("batchSizeInBytes must be between 1 and %d, got %d",
                    MAX_BATCH_SIZE_IN_BYTES, batchSizeInBytes));
        }
        this.batchSizeInBytes = batchSizeInBytes;
    }

    public int getBatchMillis() {
        return batchMillis;
    }

    public void setBatchMillis(int batchMillis) {
        if (batchMillis < MIN_BATCH_MILLIS) {
            throw new IllegalArgumentException(String.format("batchMillis must be greater than or equal to %d, got %d",
                    MIN_BATCH_MILLIS, batchMillis));
        }
        this.batchMillis = batchMillis;
    }

    public int getRetries() {
        return retries;
    }

    public void setRetries(int retries) {
        this.retries = retries;
    }

    public long getInitRetryBackoffMillis() {
        return initRetryBackoffMillis;
    }

    public void setInitRetryBackoffMillis(long initRetryBackoffMillis) {
        if (initRetryBackoffMillis <= 0) {
            throw new IllegalArgumentException("initRetryBackoffMillis must be greater than 0, got " + initRetryBackoffMillis);
        }
        this.initRetryBackoffMillis = initRetryBackoffMillis;
    }

    public long getMaxRetryBackoffMillis() {
        return maxRetryBackoffMillis;
    }

    public void setMaxRetryBackoffMillis(long maxRetryBackoffMillis) {
        if (maxRetryBackoffMillis <= 0) {
            throw new IllegalArgumentException("maxRetryBackoffMillis must be greater than 0, got " + maxRetryBackoffMillis);
        }
        this.maxRetryBackoffMillis = maxRetryBackoffMillis;
    }

    public boolean isLogConvergence() {
        return logConvergence;
    }

    public void setLogConvergence(boolean logConvergence) {
        this.logConvergence = logConvergence;
    }

    public int getLogConvergenceMillis() {
        return logConvergenceMillis;
    }

    public void setLogConvergenceMillis(int logConvergenceMillis) {
        this.logConvergenceMillis = logConvergenceMillis;
    }
}
