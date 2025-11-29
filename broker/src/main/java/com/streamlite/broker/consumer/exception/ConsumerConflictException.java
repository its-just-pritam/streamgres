package com.streamlite.broker.consumer.exception;

import lombok.Getter;

@Getter
public class ConsumerConflictException extends RuntimeException {

    private final String message;

    public ConsumerConflictException(String message) {
        super(message);
        this.message = message;
    }
}
