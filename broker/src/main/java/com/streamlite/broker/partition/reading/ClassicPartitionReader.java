package com.streamlite.broker.partition.reading;

import com.streamlite.broker.partition.dto.PartitionMessageDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;

@Slf4j
public class ClassicPartitionReader extends BasePartitionReader {


    public ClassicPartitionReader(String topicName, Integer partition, Long offset, Integer limit) {

        String partitionName = topicName + "_" + "partition" + "_" + partition;
        this.readerQuery = String.format("""
            WITH boundary_time AS (
                SELECT timestamp AS ts
                FROM %s
                WHERE msg_offset >= %s
                LIMIT 1
            )
            SELECT * FROM %s
            WHERE timestamp >= (SELECT ts FROM boundary_time)
            AND msg_offset > %s
            ORDER BY msg_offset ASC
            LIMIT %s;
            """,
            partitionName,
            offset,
            partitionName,
            offset,
            limit
        );
    }

    @Override
    public List<PartitionMessageDto> read(JdbcTemplate datastoreTemplate) {

        return datastoreTemplate.query(readerQuery, (resultSet, rowNum) -> {
            try {
                return PartitionMessageDto.builder()
                        .msgKey(new String(resultSet.getBinaryStream("msg_key").readAllBytes()))
                        .msgValue(new String(resultSet.getBinaryStream("msg_value").readAllBytes()))
                        .offset(resultSet.getLong("msg_offset"))
                        .timestamp(resultSet.getTimestamp("timestamp").toInstant().atOffset(ZoneOffset.UTC))
                        .build();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

    }

}
