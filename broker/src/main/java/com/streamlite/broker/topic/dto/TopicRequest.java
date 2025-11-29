package com.streamlite.broker.topic.dto;

import com.fasterxml.jackson.databind.JsonNode;
import com.streamlite.broker.topic.exception.TopicInvalidException;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Optional;

@Setter
@Getter
public class TopicRequest {

    private String description;
    private String environment;
    private Integer partitions;
    private Integer replicationFactor;
    private Integer retentionHours;
    private String cleanupPolicy;
    private String type;
    private String schemaType;
    private JsonNode schemaDefinition;
    private String ownerTeam;
    private List<String> tags;

    public void sanitize() {

        List<String> typeCollection = List.of("classic", "distinct", "priority");
        List<String> cleanupPolicyCollection = List.of("compact", "delete");

        environment = Optional.ofNullable(environment).orElse("dev");
        partitions = Optional.ofNullable(partitions).orElse(1);
        replicationFactor = Optional.ofNullable(replicationFactor).orElse(2);
        retentionHours = Optional.ofNullable(retentionHours).orElse(168);
        cleanupPolicy = Optional.ofNullable(cleanupPolicy).orElse("delete");
        tags = Optional.ofNullable(tags).orElse(List.of());
        type = Optional.ofNullable(type).orElse("classic");

        if(!typeCollection.contains(type))
            throw new TopicInvalidException("Topic type supported for [" + String.join(",", typeCollection) + "]");
        if (partitions > 100)
            throw new TopicInvalidException("Max partitions per topic is 100");
        if(retentionHours > 720)
            throw new TopicInvalidException("Max retention period per topic is 720 hours");
        if(!cleanupPolicyCollection.contains(cleanupPolicy))
            throw new TopicInvalidException("Topic cleanup policy supported for [" + String.join(",", cleanupPolicyCollection) + "]");
        if(replicationFactor > 3)
            throw new TopicInvalidException("Max replication factor per topic is 3");
    }

}
