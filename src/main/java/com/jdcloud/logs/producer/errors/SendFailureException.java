package com.jdcloud.logs.producer.errors;

import com.jdcloud.logs.producer.res.Attempt;
import com.jdcloud.logs.producer.res.Response;

import java.util.List;

/**
 * 发送失败
 *
 * @author liubai
 * @date 2022/11/25
 */
public class SendFailureException extends ProducerException {

    private final Response response;

    public SendFailureException(Response response) {
        super(response.getErrorMessage());
        this.response = response;
    }

    public Response getResponse() {
        return response;
    }

    public List<Attempt> getAttempts() {
        return response.getAttempts();
    }

    public int getAttemptCount() {
        return response.getAttemptCount();
    }

    public String getErrorCode() {
        return response.getErrorCode();
    }

    public String getErrorMessage() {
        return response.getErrorMessage();
    }
}
