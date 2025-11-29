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
@Table(name = "deleted_topic_metadata")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DeletedTopicMetadata {

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

    @Column(nullable = false, columnDefinition = "integer")
    private Integer partitions;

    @Column(nullable = false)
    private Integer replicationFactor;

    @Column(columnDefinition = "integer")
    private Integer retentionHours;

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

}
