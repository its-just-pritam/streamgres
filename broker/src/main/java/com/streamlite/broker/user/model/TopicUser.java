package com.streamlite.broker.user.model;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.OffsetDateTime;
import java.util.UUID;

@Entity
@Table(name = "topic_user")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TopicUser {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(nullable = false, columnDefinition = "uuid DEFAULT gen_random_uuid()")
    private UUID id;

    @Column(name = "tenant_id", nullable = false)
    private String tenantId;

    @CreationTimestamp
    @Column(name = "created_at", columnDefinition = "timestamptz default now()")
    private OffsetDateTime createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", columnDefinition = "timestamptz default now()")
    private OffsetDateTime updatedAt;

    @Column(name = "public_key_pem", nullable = false)
    private String publicKeyPem;

}
