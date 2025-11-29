package com.streamlite.broker.partition.ingestion;

import com.streamlite.broker.partition.dto.PartitionMessageDto;
import lombok.Getter;
import lombok.Setter;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ClassicPartitionIngestion extends BasePartitionIngestion {

    public ClassicPartitionIngestion(String topicName, Integer partition, List<PartitionMessageDto> partitionMessageDtoList) {

        ingestionQueries = new ArrayList<>();
        String partitionName = topicName + "_" + "partition" + "_" + partition;

        for(PartitionMessageDto partitionMessageDto: partitionMessageDtoList) {

            String ingestionQuery = String.format("""
            INSERT INTO %s (
                timestamp,
                msg_key,
                msg_value,
                msg_headers
            ) VALUES (
                '%s',        -- timestamp
                '%s',        -- msg_key (BYTEA hex)
                '%s',        -- msg_value (BYTEA hex)
                '%s'         -- JSONB
            );
            """,
            partitionName,
            partitionMessageDto.getTimestamp(),
            partitionMessageDto.getMsgKey(),
            partitionMessageDto.getMsgValue(),
            partitionMessageDto.getMsgHeaders()
            );

            ingestionQueries.add(ingestionQuery);
        }
    }

}
