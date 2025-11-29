package com.streamlite.broker.partition.ingestion;

import com.streamlite.broker.partition.exception.PartitionIngestionException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Slf4j
public abstract class BasePartitionIngestion {

    protected List<String> ingestionQueries;

    public void ingest(JdbcTemplate datastoreTemplate, Integer retryLimit) {

        if(retryLimit < 0)
            throw new PartitionIngestionException("Failed to ingest message");

        try {
            for(String query: ingestionQueries) {
                datastoreTemplate.execute(query);
            }
        } catch (Exception e) {
            log.error("Error ingesting: {}", e.getMessage(), e);
            log.info("Retry attempts left: {}", retryLimit);
            ingest(datastoreTemplate, retryLimit-1);
        }

    }

}
