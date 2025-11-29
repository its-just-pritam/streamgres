package com.streamlite.broker.consumer.dto.session;

import lombok.Getter;
import lombok.Setter;

import java.time.OffsetDateTime;
import java.util.Map;

@Getter
@Setter
public class LivePartition {

    private String user;
    private String topic;
    private String consumer;
    private Integer partition;
    private String session;
    private String instanceId;
    private Long offset;
    private Long timeout;
    private OffsetDateTime lastUpdated;

}
