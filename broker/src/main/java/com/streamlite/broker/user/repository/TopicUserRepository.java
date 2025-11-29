package com.streamlite.broker.user.repository;

import com.streamlite.broker.topic.model.TopicMetadata;
import com.streamlite.broker.user.model.TopicUser;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface TopicUserRepository extends JpaRepository<TopicUser, UUID> {

    @Override
    Optional<TopicUser> findById(UUID uuid);

    @Query("select t from TopicUser t where t.tenantId = :tenantId")
    Optional<TopicUser> findByTenantId(@Param("tenantId") String tenantId);
}
