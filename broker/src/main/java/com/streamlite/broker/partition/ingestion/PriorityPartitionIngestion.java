package com.streamlite.broker.partition.ingestion;

import com.streamlite.broker.partition.dto.PartitionMessageDto;

import java.util.ArrayList;
import java.util.List;

public class PriorityPartitionIngestion extends BasePartitionIngestion {

    public PriorityPartitionIngestion(String topicName, Integer partition, List<PartitionMessageDto> partitionMessageDtoList) {

        ingestionQueries = new ArrayList<>();
        String partitionName = topicName + "_" + "partition" + "_" + partition;

        for(PartitionMessageDto partitionMessageDto: partitionMessageDtoList) {

            String ingestionQuery = String.format("""
            INSERT INTO %s (
                priority,
                timestamp,
                msg_key,
                msg_value,
                msg_headers
            ) VALUES (
                '%d',        -- priority
                '%s',        -- timestamp
                '%s',        -- msg_key (BYTEA hex)
                '%s',        -- msg_value (BYTEA hex)
                '%s'         -- JSONB
            );
            """,
            partitionName,
            partitionMessageDto.getPriority(),
            partitionMessageDto.getTimestamp(),
            partitionMessageDto.getMsgKey(),
            partitionMessageDto.getMsgValue(),
            partitionMessageDto.getMsgHeaders()
            );

            ingestionQueries.add(ingestionQuery);
        }
    }

}
