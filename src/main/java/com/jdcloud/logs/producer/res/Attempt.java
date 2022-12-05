package com.jdcloud.logs.producer.res;

/**
 * 行为追溯
 *
 * @author liubai
 * @date 2022/11/21
 */
public class Attempt {

    private final boolean successful;

    private final String requestId;

    private final String errorCode;

    private final String errorMessage;

    private final long timestampMills;

    public Attempt(boolean successful, String requestId, String errorCode, String errorMessage, long timestampMills) {
        this.successful = successful;
        this.requestId = requestId;
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
        this.timestampMills = timestampMills;
    }

    public boolean isSuccessful() {
        return successful;
    }

    public String getRequestId() {
        return requestId;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public long getTimestampMills() {
        return timestampMills;
    }

    @Override
    public String toString() {
        return "Attempt{" +
                "successful=" + successful +
                ", requestId='" + requestId + '\'' +
                ", errorCode='" + errorCode + '\'' +
                ", errorMessage='" + errorMessage + '\'' +
                ", timestampMills=" + timestampMills +
                '}';
    }
}
