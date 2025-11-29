package com.streamlite.broker.topic.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.streamlite.broker.topic.dto.TopicRequest;
import com.vladmihalcea.hibernate.type.json.JsonType;
import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.Type;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.UUID;

@Entity
@Table(name = "topic_metadata")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TopicMetadata {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(nullable = false, columnDefinition = "uuid DEFAULT gen_random_uuid()")
    private UUID id;

    @Column(name = "topic_name", nullable = false)
    private String topicName;

    @Column(columnDefinition = "text")
    private String description;

    @Column(columnDefinition = "text")
    private String environment;

    @Column(columnDefinition = "userId")
    private UUID userId;

    @Builder.Default
    @Column(nullable = false, columnDefinition = "integer default 1")
    private Integer partitions = 1;

    @Column(nullable = false)
    private Integer replicationFactor;

    @Builder.Default
    @Column(columnDefinition = "integer default 168")
    private Integer retentionHours = 168;

    @Column(columnDefinition = "text")
    private String cleanupPolicy;

    @Column(columnDefinition = "text")
    private String type;

    @Column(columnDefinition = "text")
    private String schemaType;

    @Type(JsonType.class)
    @Column(columnDefinition = "jsonb")
    private JsonNode schemaDefinition;

    @Column(columnDefinition = "text")
    private String ownerTeam;

    @Column(name = "tags", columnDefinition = "text[]")
    private String[] tags;

    @CreationTimestamp
    @Column(name = "created_at", columnDefinition = "timestamptz default now()")
    private OffsetDateTime createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", columnDefinition = "timestamptz default now()")
    private OffsetDateTime updatedAt;

    public TopicRequest buildRequest() {
        TopicRequest topicRequest = new TopicRequest();
        topicRequest.setDescription(description);
        topicRequest.setEnvironment(environment);
        topicRequest.setPartitions(partitions);
        topicRequest.setReplicationFactor(replicationFactor);
        topicRequest.setRetentionHours(retentionHours);
        topicRequest.setCleanupPolicy(cleanupPolicy);
        topicRequest.setType(type);
        topicRequest.setSchemaType(schemaType);
        topicRequest.setSchemaDefinition(schemaDefinition);
        topicRequest.setOwnerTeam(ownerTeam);
        topicRequest.setTags(Arrays.stream(tags).toList());
        return topicRequest;
    }

    public DeletedTopicMetadata buildDeletedTopicMetadata() {
        return DeletedTopicMetadata.builder()
                .topicName(topicName)
                .description(description)
                .environment(environment)
                .userId(userId)
                .partitions(partitions)
                .replicationFactor(replicationFactor)
                .retentionHours(retentionHours)
                .cleanupPolicy(cleanupPolicy)
                .type(type)
                .schemaType(schemaType)
                .schemaDefinition(schemaDefinition)
                .ownerTeam(ownerTeam)
                .tags(tags)
                .createdAt(OffsetDateTime.now())
                .updatedAt(OffsetDateTime.now())
                .build();
    }
}
