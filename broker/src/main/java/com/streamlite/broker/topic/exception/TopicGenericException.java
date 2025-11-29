package com.streamlite.broker.topic.exception;

import lombok.Getter;

@Getter
public class TopicGenericException extends RuntimeException {

    private final String message;

    public TopicGenericException(String message) {
        super(message);
        this.message = message;
    }
}
