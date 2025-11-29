package com.streamlite.broker.partition.exception;

import lombok.Getter;

@Getter
public class PartitionBuildException extends RuntimeException {

    private final String message;

    public PartitionBuildException(String message) {
        super(message);
        this.message = message;
    }
}
