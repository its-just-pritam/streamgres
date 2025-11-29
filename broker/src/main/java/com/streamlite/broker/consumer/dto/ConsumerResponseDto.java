package com.streamlite.broker.consumer.dto;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Setter
@Getter
public class ConsumerResponseDto {

    private String topic;
    private String name;
    private List<PartitionSpecDto> partitionSpec;


}
