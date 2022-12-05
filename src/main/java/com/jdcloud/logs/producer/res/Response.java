package com.jdcloud.logs.producer.res;

import com.google.common.collect.Iterables;

import java.util.List;

/**
 * 发送响应
 *
 * @author liubai
 * @date 2022/11/21
 */
public class Response {

    private final boolean successful;

    private final List<Attempt> attempts;

    private final int attemptCount;

    public Response(boolean successful, List<Attempt> attempts, int attemptCount) {
        this.successful = successful;
        this.attempts = attempts;
        this.attemptCount = attemptCount;
    }

    public boolean isSuccessful() {
        return successful;
    }

    public List<Attempt> getAttempts() {
        return attempts;
    }

    public int getAttemptCount() {
        return attemptCount;
    }

    public String getErrorCode() {
        Attempt lastAttempt = Iterables.getLast(attempts);
        return lastAttempt.getErrorCode();
    }

    public String getErrorMessage() {
        Attempt lastAttempt = Iterables.getLast(attempts);
        return lastAttempt.getErrorMessage();
    }

    @Override
    public String toString() {
        return "Response{" +
                "successful=" + successful +
                ", attempts=" + attempts +
                ", attemptCount=" + attemptCount +
                '}';
    }
}
