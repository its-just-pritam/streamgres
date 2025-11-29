package com.streamlite.broker.partition;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class PartitionFactory {

    public static List<String> buildConsumerQueries(String type, String topicName, Integer count, Integer retention) {
        if(Objects.equals(type, "basic"))
            return generateBasicPartitionQueries(topicName,count,retention);
        else
            return List.of();
    }

    private static List<String> generateBasicPartitionQueries(String topicName, Integer count, Integer retention) {

        List<String> queries = new ArrayList<>();
        for(int partition=0; partition<count; ++partition) {
            String partitionName = topicName + "_" + "partition" + "_" + partition;
            String tableQuery = String.format("""
                    CREATE TABLE IF NOT EXISTS %s (
                        msg_offset      BIGSERIAL NOT NULL,
                        timestamp       TIMESTAMPTZ NOT NULL,
                        msg_key         BYTEA,
                        msg_value       BYTEA,
                        msg_headers     JSONB,
                        created_at      TIMESTAMPTZ DEFAULT NOW(),
                        PRIMARY KEY (msg_offset, timestamp)
                    );
                """, partitionName);
            String indexQuery = String.format("""
                    CREATE INDEX ON %s (timestamp);
                """, partitionName);
            String hyperTableQuery = String.format("""
                    SELECT create_hypertable('%s', 'timestamp');
                """, partitionName);
            String retentionQuery = String.format("""
                    SELECT drop_chunks('%s', INTERVAL '%d hours');
                """, partitionName, retention);

            queries.add(tableQuery);
            queries.add(indexQuery);
            queries.add(hyperTableQuery);
            queries.add(retentionQuery);
        }

        return queries;

    }

}
