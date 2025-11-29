package com.streamlite.broker.partition.ingestion;

import com.streamlite.broker.partition.dto.PartitionMessageDto;

import java.util.ArrayList;
import java.util.List;

public class DistinctPartitionIngestion extends ClassicPartitionIngestion {

    public DistinctPartitionIngestion(String topicName, Integer partition, List<PartitionMessageDto> partitionMessageDtoList) {
        super(topicName,partition,partitionMessageDtoList);
    }

}
