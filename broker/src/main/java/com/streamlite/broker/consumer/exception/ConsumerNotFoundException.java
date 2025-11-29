package com.streamlite.broker.consumer.exception;

import lombok.Getter;

@Getter
public class ConsumerNotFoundException extends RuntimeException {

    private final String message;

    public ConsumerNotFoundException(String message) {
        super(message);
        this.message = message;
    }
}
