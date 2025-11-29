package com.streamlite.broker.consumer.dto.session;

import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
public class LiveUser {

    private Map<String, LiveTopic> topics;

    public LiveUser() {
        this.topics = new HashMap<>();
    }
}
