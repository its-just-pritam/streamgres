package com.streamlite.broker.consumer.dto.session;

import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
public class LiveTopic {

    private Map<String , LiveConsumerGroup> consumerGroups;

    public LiveTopic() {
        this.consumerGroups = new HashMap<>();
    }
}
