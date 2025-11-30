package com.streamlite.broker.partition.ingestion.factory;

import com.streamlite.broker.partition.dto.PartitionMessageDto;
import com.streamlite.broker.partition.ingestion.*;

import java.util.List;
import java.util.Objects;

public class PartitionIngestionFactory {

    public static BasePartitionIngestion get(String type, String topicName, int partition, List<PartitionMessageDto> partitionMessageDtoList) {

        if(Objects.equals(type, "classic"))
            return new ClassicPartitionIngestion(topicName,partition,partitionMessageDtoList);
//        else if(Objects.equals(type, "distinct"))
//            return new DistinctPartitionIngestion(topicName,partition,partitionMessageDtoList);
        else if(Objects.equals(type, "priority"))
            return new PriorityPartitionIngestion(topicName,partition,partitionMessageDtoList);
        else
            return new NullPartitionIngestion();

    }

}
