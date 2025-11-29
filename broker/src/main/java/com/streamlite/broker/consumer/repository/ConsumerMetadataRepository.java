package com.streamlite.broker.consumer.repository;

import com.streamlite.broker.consumer.model.ConsumerMetadata;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface ConsumerMetadataRepository extends JpaRepository<ConsumerMetadata, UUID> {

    @Query("select c from ConsumerMetadata c where c.userId = :userId and c.topicId = :topicId and c.name = :name")
    List<ConsumerMetadata> findByUserIdAndTopicIdAndName(
            @Param("userId") UUID userId,
            @Param("topicId") UUID topicId,
            @Param("name") String name
    );

    @Query("""
            select c from ConsumerMetadata c
            where c.userId = :userId and c.topicId = :topicId and c.name = :name and c.partitionId = :partitionId""")
    Optional<ConsumerMetadata> findByUserIdAndTopicIdAndNameAndPartitionId(
            @Param("userId") UUID userId,
            @Param("topicId") UUID topicId,
            @Param("name") String name,
            @Param("partitionId") Integer partitionId
    );

    @Query("select c from ConsumerMetadata c where c.userId = :userId and c.topicId = :topicId")
    List<ConsumerMetadata> findByUserIdAndTopicId(
            @Param("userId") UUID userId,
            @Param("topicId") UUID topicId
    );
}
