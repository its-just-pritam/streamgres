package com.streamlite.broker.partition.reading;

import com.streamlite.broker.partition.dto.PartitionMessageDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;

@Slf4j
public class NullPartitionReader extends BasePartitionReader {


    public NullPartitionReader() {
        this.readerQuery = "";
    }

    @Override
    public List<PartitionMessageDto> read(JdbcTemplate datastoreTemplate) {
        return List.of();
    }

}
