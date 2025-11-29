package com.streamlite.broker.topic.repository;

import com.streamlite.broker.topic.model.TopicMetadata;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface TopicMetadataRepository extends JpaRepository<TopicMetadata, UUID> {

    @Override
    Optional<TopicMetadata> findById(UUID uuid);

    @Query("select t from TopicMetadata t where t.userId = :userId and t.topicName = :topicName")
    Optional<TopicMetadata> findByUserIdAndTopicName(@Param("userId") UUID userId, @Param("topicName") String topicName);

    @Query("select t from TopicMetadata t where t.userId = :userId")
    List<TopicMetadata> findByUserId(@Param("userId") UUID userId);
}
