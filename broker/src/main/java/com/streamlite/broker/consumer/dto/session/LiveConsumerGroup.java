package com.streamlite.broker.consumer.dto.session;

import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
public class LiveConsumerGroup {

    private Map<Integer, LivePartition> partitions;

    public LiveConsumerGroup() {
        this.partitions = new HashMap<>();
    }
}
