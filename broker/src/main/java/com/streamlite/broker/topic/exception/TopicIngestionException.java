package com.streamlite.broker.topic.exception;

import lombok.Getter;

@Getter
public class TopicIngestionException extends RuntimeException {

    private final String message;

    public TopicIngestionException(String message) {
        super(message);
        this.message = message;
    }
}
