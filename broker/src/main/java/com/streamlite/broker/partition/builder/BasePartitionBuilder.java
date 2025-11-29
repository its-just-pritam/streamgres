package com.streamlite.broker.partition.builder;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class BasePartitionBuilder {

    protected Map<Integer, List<String>> builderQueries;
    protected Map<Integer, List<String>> rollbackQueries;

    public void build(JdbcTemplate datastoreTemplate) {

        AtomicInteger current = new AtomicInteger(0);
        try {
            builderQueries.forEach((partition, queries) -> {
                for (String query: queries) {
                    datastoreTemplate.execute(query);
                }
                current.incrementAndGet();
            });
        } catch (Exception e) {
            rollback(datastoreTemplate, current.get());
            throw e;
        }

    }

    @Async
    protected void rollback(JdbcTemplate datastoreTemplate, Integer currentPartition) {
        for(int i = currentPartition; i>=0; --i) {
            for(String query: rollbackQueries.get(i)) {
                datastoreTemplate.execute(query);
            }
        }
    }

}
