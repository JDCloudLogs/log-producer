package com.jdcloud.logs.producer.errors;

public class LogSizeTooLargeException extends ProducerException {

  public LogSizeTooLargeException() {
    super();
  }

  public LogSizeTooLargeException(String message, Throwable cause) {
    super(message, cause);
  }

  public LogSizeTooLargeException(String message) {
    super(message);
  }

  public LogSizeTooLargeException(Throwable cause) {
    super(cause);
  }
}
