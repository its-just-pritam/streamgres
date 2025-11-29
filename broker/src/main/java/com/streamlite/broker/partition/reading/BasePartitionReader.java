package com.streamlite.broker.partition.reading;

import com.streamlite.broker.partition.dto.PartitionMessageDto;
import com.streamlite.broker.partition.exception.PartitionIngestionException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;

@Slf4j
public abstract class BasePartitionReader {

    protected String readerQuery;

    public abstract List<PartitionMessageDto> read(JdbcTemplate datastoreTemplate);

}
