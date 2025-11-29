package com.streamlite.broker.topic.dto;

import com.fasterxml.jackson.databind.JsonNode;
import com.streamlite.broker.topic.exception.TopicIngestionException;
import com.vladmihalcea.hibernate.util.StringUtils;
import lombok.Getter;
import lombok.Setter;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Setter
@Getter
public class TopicIngestionRequest {

    private String messageKey;
    private String message;
    private OffsetDateTime timestamp;
    private Map<String, String> headers;
    private Integer priority;

    public void sanitize(String topicName) {

        timestamp = Optional.ofNullable(timestamp).orElse(OffsetDateTime.now());
        if(StringUtils.isBlank(message))
            throw new TopicIngestionException("Invalid message fo topic " + topicName);

    }

}
