package com.streamlite.broker.topic.exception;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
public class TopicInvalidException extends RuntimeException {

    private final String message;

    public TopicInvalidException(String message) {
        super(message);
        this.message = message;
    }
}
