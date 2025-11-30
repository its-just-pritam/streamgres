package com.streamlite.broker.topic.service;

import com.streamlite.broker.partition.builder.*;
import com.streamlite.broker.topic.dto.TopicRequest;
import com.streamlite.broker.topic.exception.TopicConflictException;
import com.streamlite.broker.topic.exception.TopicGenericException;
import com.streamlite.broker.topic.exception.TopicInvalidException;
import com.streamlite.broker.topic.exception.TopicNotFoundException;
import com.streamlite.broker.topic.model.DeletedTopicMetadata;
import com.streamlite.broker.topic.model.TopicMetadata;
import com.streamlite.broker.topic.repository.DeletedTopicMetadataRepository;
import com.streamlite.broker.topic.repository.TopicMetadataRepository;
import com.streamlite.broker.user.exception.UserNotFoundException;
import com.streamlite.broker.user.model.TopicUser;
import com.streamlite.broker.user.repository.TopicUserRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

@Slf4j
@Service
public class TopicManagerService {

    private final JdbcTemplate datastoreTemplate;
    private final List<String> reservedTables;
    private final TopicMetadataRepository topicMetadataRepository;
    private final DeletedTopicMetadataRepository deletedTopicMetadataRepository;
    private final TopicUserRepository topicUserRepository;

    public TopicManagerService(
            @Autowired
            @Qualifier("datastoreTemplate")
            JdbcTemplate datastoreTemplate,
            @Autowired
            TopicMetadataRepository topicMetadataRepository,
            @Autowired
            DeletedTopicMetadataRepository deletedTopicMetadataRepository,
            @Autowired
            TopicUserRepository topicUserRepository) {
        this.datastoreTemplate = datastoreTemplate;
        this.topicMetadataRepository = topicMetadataRepository;
        this.deletedTopicMetadataRepository = deletedTopicMetadataRepository;
        this.topicUserRepository = topicUserRepository;
        this.reservedTables = List.of("partitions_metadata", "topic_metadata");
    }

    public void createTopic(String topicName, TopicRequest topicRequest, String tenantId) {

        if (!topicName.matches("[a-zA-Z0-9_]+"))
            throw new TopicInvalidException("Invalid topic name: " + topicName);

        Optional<TopicUser> topicUser = topicUserRepository.findByTenantId(tenantId);
        if(topicUser.isEmpty())
            throw new UserNotFoundException("User " + tenantId + " not found!");

        if(topicMetadataRepository.findByUserIdAndTopicName(topicUser.get().getId(), topicName).isPresent())
            throw new TopicConflictException("Topic " + topicName + " already exists!");
        if(deletedTopicMetadataRepository.findOneByTopicName(topicName).isPresent())
            throw new TopicConflictException("Topic " + topicName + " is soft-deleted. Please try another name!");

        try {
            createPhysicalPartitions(topicName, topicRequest.getPartitions(), topicRequest.getRetentionHours(), topicRequest.getType());
        } catch (Exception e) {
            throw new TopicGenericException(e.getMessage());
        }

        try {
            createTopicMetadata(topicName, topicRequest, topicUser.get().getId());
        } catch (Exception e) {
            log.error("[{}][{}]: Failed to create topic metadata for topicName: {}",
                    this.getClass().getSimpleName(), "createPhysicalTopic", topicName, e);
            deletePhysicalPartitions(topicName, topicRequest.getPartitions(), topicRequest.getCleanupPolicy());
            throw new TopicGenericException(e.getMessage());
        }
    }

    private void createPhysicalPartitions(String topicName, Integer count, Integer retention, String type) {
        BasePartitionBuilder partitionBuilder;

        if(Objects.equals(type, "classic"))
            partitionBuilder = new ClassicPartitionBuilder(topicName, count, retention);
//        else if(Objects.equals(type, "distinct"))
//            partitionBuilder = new DistinctPartitionBuilder(topicName, count, retention);
        else if(Objects.equals(type, "priority"))
            partitionBuilder = new PriorityPartitionBuilder(topicName, count, retention);
        else
            partitionBuilder = new NullPartitionBuilder();

        partitionBuilder.build(datastoreTemplate);
    }

    private void createTopicMetadata(String topicName, TopicRequest topicRequest, UUID userId) {

        TopicMetadata topicMetadata = TopicMetadata.builder()
                .createdAt(OffsetDateTime.now())
                .partitions(topicRequest.getPartitions())
                .updatedAt(OffsetDateTime.now())
                .cleanupPolicy(topicRequest.getCleanupPolicy())
                .description(topicRequest.getDescription())
                .topicName(topicName)
                .environment(topicRequest.getEnvironment())
                .userId(userId)
                .type(topicRequest.getType())
                .tags(topicRequest.getTags().toArray(new String[0]))
                .ownerTeam(topicRequest.getOwnerTeam())
                .replicationFactor(topicRequest.getReplicationFactor())
                .schemaType(topicRequest.getSchemaType())
                .schemaDefinition(topicRequest.getSchemaDefinition())
                .build();

        topicMetadataRepository.save(topicMetadata);
    }

    public void deleteTopic(String topicName, String tenantId) {
        if (!topicName.matches("[a-zA-Z0-9_]+") || reservedTables.contains(topicName)) {
            throw new TopicInvalidException("Invalid table name: " + topicName);
        }

        Optional<TopicUser> topicUser = topicUserRepository.findByTenantId(tenantId);
        if(topicUser.isEmpty())
            throw new UserNotFoundException("User " + tenantId + " not found!");

        Optional<TopicMetadata> topicMetadata = topicMetadataRepository.findByUserIdAndTopicName(topicUser.get().getId(), topicName);
        if(topicMetadata.isEmpty())
            throw new TopicNotFoundException("Topic " + topicName + " does not exist!");

        try {
            deleteTopicMetadata(topicMetadata.get());
        } catch (Exception e) {
            throw new TopicGenericException(e.getMessage());
        }

        try {
            deletePhysicalPartitions(topicName, topicMetadata.get().getPartitions(), topicMetadata.get().getCleanupPolicy());
        } catch (Exception e) {
            log.error("[{}][{}]: Failed to delete physical topic for topicName: {}",
                    this.getClass().getSimpleName(), "createPhysicalTopic", topicName, e);
            createTopicMetadata(topicName, topicMetadata.get().buildRequest(), topicUser.get().getId());
            throw new TopicGenericException(e.getMessage());
        }

    }

    public void hardDeleteTopic(String topicName, String tenantId) {
        if (!topicName.matches("[a-zA-Z0-9_]+") || reservedTables.contains(topicName)) {
            throw new TopicInvalidException("Invalid table name: " + topicName);
        }

        Optional<TopicUser> topicUser = topicUserRepository.findByTenantId(tenantId);
        if(topicUser.isEmpty())
            throw new UserNotFoundException("User " + tenantId + " not found!");

        int partitions = 0;
        String cleanupPolicy;

        Optional<TopicMetadata> topicMetadata = topicMetadataRepository.findByUserIdAndTopicName(topicUser.get().getId(), topicName);
        if(topicMetadata.isPresent()) {
            partitions = topicMetadata.get().getPartitions();
            cleanupPolicy = topicMetadata.get().getCleanupPolicy();

            try {
                topicMetadataRepository.deleteById(topicMetadata.get().getId());
            } catch (Exception e) {
                throw new TopicGenericException(e.getMessage());
            }
        } else {
            List<DeletedTopicMetadata> deletedTopicMetadataList = deletedTopicMetadataRepository.findByTopicName(topicName);
            if(deletedTopicMetadataList.isEmpty())
                throw new TopicNotFoundException("Topic " + topicName + " does not exist!");

            cleanupPolicy = "delete";
            try {
                for (DeletedTopicMetadata deletedTopicMetadata: deletedTopicMetadataList) {
                    partitions = Math.max(partitions, deletedTopicMetadata.getPartitions());
                    deletedTopicMetadataRepository.deleteById(deletedTopicMetadata.getId());
                }
            } catch (Exception e) {
                throw new TopicGenericException(e.getMessage());
            }
        }

        try {
            deletePhysicalPartitions(topicName, partitions, cleanupPolicy);
        } catch (Exception e) {
            log.error("[{}][{}]: Failed to delete physical topic for topicName: {}",
                    this.getClass().getSimpleName(), "createPhysicalTopic", topicName, e);
            throw new TopicGenericException(e.getMessage());
        }

    }

    private void deleteTopicMetadata(TopicMetadata topicMetadata) {
        if(Objects.equals(topicMetadata.getCleanupPolicy(), "delete"))
            topicMetadataRepository.deleteById(topicMetadata.getId());
        else if(Objects.equals(topicMetadata.getCleanupPolicy(), "compact")) {
            deletedTopicMetadataRepository.save(topicMetadata.buildDeletedTopicMetadata());
            topicMetadataRepository.deleteById(topicMetadata.getId());
        }
    }

    private void deletePhysicalPartitions(String topicName, Integer partitions, String cleanupPolicy) {

        if(Objects.equals(cleanupPolicy, "delete")) {
            for (int partition=0; partition<partitions; ++partition) {
                String partitionName = topicName + "_" + "partition" + "_" + partition;
                String sql = String.format("DROP TABLE IF EXISTS %s;", partitionName);
                datastoreTemplate.execute(sql);
            }
        } else if(Objects.equals(cleanupPolicy, "compact")) {
            for (int partition=0; partition<partitions; ++partition) {
                String indexName = topicName + "_" + "partition" + "_" + partition + "_timestamp_idx";
                String sql = String.format("DROP INDEX IF EXISTS %s;", indexName);
                datastoreTemplate.execute(sql);
            }
        }
    }

    public TopicMetadata getTopic(String topicName, String tenantId) {

        Optional<TopicUser> topicUser = topicUserRepository.findByTenantId(tenantId);
        if(topicUser.isEmpty())
            throw new UserNotFoundException("User " + tenantId + " not found!");

        Optional<TopicMetadata> topicMetadata = topicMetadataRepository.findByUserIdAndTopicName(topicUser.get().getId(), topicName);
        if(topicMetadata.isEmpty())
            throw new TopicNotFoundException("Topic " + topicName + " does not exist!");

        return topicMetadata.get();
    }

    public List<TopicMetadata> listTopics(String tenantId) {
        Optional<TopicUser> topicUser = topicUserRepository.findByTenantId(tenantId);
        if(topicUser.isEmpty())
            throw new UserNotFoundException("User " + tenantId + " not found!");

        return topicMetadataRepository.findByUserId(topicUser.get().getId());
    }
}