package com.streamlite.broker.consumer.model;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.OffsetDateTime;
import java.util.UUID;

@Entity
@Table(name = "consumer_metadata")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ConsumerMetadata {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(nullable = false, columnDefinition = "uuid DEFAULT gen_random_uuid()")
    private UUID id;

    @Column(name = "name")
    private String name;

    @Column(name = "user_id")
    private UUID userId;

    @Column(name = "topic_id")
    private UUID topicId;

    @Column(name = "partition_id", nullable = false)
    private Integer partitionId;

    @Builder.Default
    @Column(name = "partition_offset", nullable = false)
    private Long partitionOffset = 0L;

    @Column(name = "leader_broker", columnDefinition = "text")
    private String leaderBroker;

    @CreationTimestamp
    @Column(name = "created_at", columnDefinition = "timestamptz default now()")
    private OffsetDateTime createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", columnDefinition = "timestamptz default now()")
    private OffsetDateTime updatedAt;
}
