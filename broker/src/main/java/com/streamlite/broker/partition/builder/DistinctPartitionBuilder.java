package com.streamlite.broker.partition.builder;

import java.util.ArrayList;
import java.util.HashMap;

public class DistinctPartitionBuilder extends BasePartitionBuilder {

    public DistinctPartitionBuilder(String topicName, Integer count, Integer retention) {

        builderQueries = new HashMap<>();
        rollbackQueries = new HashMap<>();

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
            String msgKeyIndexQuery = String.format("""
                    CREATE INDEX ON %s (msg_key);
                """, partitionName);
            String hyperTableQuery = String.format("""
                    SELECT create_hypertable('%s', 'timestamp');
                """, partitionName);
            String retentionQuery = String.format("""
                    SELECT drop_chunks('%s', INTERVAL '%d hours');
                """, partitionName, retention);

            builderQueries.put(partition, new ArrayList<>());
            builderQueries.get(partition).add(tableQuery);
            builderQueries.get(partition).add(indexQuery);
            builderQueries.get(partition).add(msgKeyIndexQuery);
            builderQueries.get(partition).add(hyperTableQuery);
            builderQueries.get(partition).add(retentionQuery);
        }

        for (int partition=0; partition<count; ++partition) {
            String partitionName = topicName + "_" + "partition" + "_" + partition;
            String tableDropQuery = String.format("DROP TABLE IF EXISTS %s;", partitionName);
            rollbackQueries.put(partition, new ArrayList<>());
            rollbackQueries.get(partition).add(tableDropQuery);
        }

    }

}
