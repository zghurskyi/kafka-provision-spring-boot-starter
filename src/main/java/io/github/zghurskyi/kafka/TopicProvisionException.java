package io.github.zghurskyi.kafka;

public class TopicProvisionException extends RuntimeException {
    public TopicProvisionException(String message, Throwable throwable) {
        super(message, throwable);
    }
}