package com.streamlite.broker.topic.exception;

import lombok.Getter;

@Getter
public class TopicConflictException extends RuntimeException {

    private final String message;

    public TopicConflictException(String message) {
        super(message);
        this.message = message;
    }
}
