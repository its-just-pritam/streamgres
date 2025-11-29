package com.streamlite.broker.partition.exception;

import lombok.Getter;

@Getter
public class PartitionIngestionException extends RuntimeException {

    private final String message;

    public PartitionIngestionException(String message) {
        super(message);
        this.message = message;
    }
}
