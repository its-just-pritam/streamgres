package com.streamlite.broker.topic.exception;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.springframework.http.HttpStatus;

@Getter
public class TopicNotFoundException extends RuntimeException {

    private final String message;

    public TopicNotFoundException(String message) {
        super(message);
        this.message = message;
    }
}
