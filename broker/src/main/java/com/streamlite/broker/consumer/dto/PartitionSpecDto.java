package com.streamlite.broker.consumer.dto;

import lombok.Getter;
import lombok.Setter;

import java.time.OffsetDateTime;

@Setter
@Getter
public class PartitionSpecDto {

    private Integer id;
    private Long offset;
    private OffsetDateTime createdAt;
    private OffsetDateTime updatedAt;

}
