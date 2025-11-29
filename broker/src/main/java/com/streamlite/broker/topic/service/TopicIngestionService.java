package com.streamlite.broker.topic.service;

import com.streamlite.broker.partition.PartitionHashGenerator;
import com.streamlite.broker.partition.dto.PartitionMessageDto;
import com.streamlite.broker.partition.exception.PartitionIngestionException;
import com.streamlite.broker.partition.ingestion.BasePartitionIngestion;
import com.streamlite.broker.partition.ingestion.factory.PartitionIngestionFactory;
import com.streamlite.broker.topic.dto.TopicIngestionRequest;
import com.streamlite.broker.topic.exception.TopicNotFoundException;
import com.streamlite.broker.topic.model.TopicMetadata;
import com.streamlite.broker.topic.repository.TopicMetadataRepository;
import com.streamlite.broker.user.exception.UserNotFoundException;
import com.streamlite.broker.user.model.TopicUser;
import com.streamlite.broker.user.repository.TopicUserRepository;
import jakarta.transaction.Transactional;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
public class TopicIngestionService {

    private final JdbcTemplate datastoreTemplate;
    private final TopicMetadataRepository topicMetadataRepository;
    private final TopicUserRepository topicUserRepository;

    public TopicIngestionService(
            @Autowired
            JdbcTemplate datastoreTemplate,
            @Autowired
            TopicMetadataRepository topicMetadataRepository,
            @Autowired
            TopicUserRepository topicUserRepository) {
        this.datastoreTemplate = datastoreTemplate;
        this.topicMetadataRepository = topicMetadataRepository;
        this.topicUserRepository = topicUserRepository;
    }

    @Transactional(rollbackOn = PartitionIngestionException.class)
    public void ingest(String tenantId, String topicName, List<TopicIngestionRequest> topicIngestionRequestList) {

        Optional<TopicUser> topicUser = topicUserRepository.findByTenantId(tenantId);
        if(topicUser.isEmpty())
            throw new UserNotFoundException("User " + tenantId + " not found!");

        Optional<TopicMetadata> topicMetadata = topicMetadataRepository.findByUserIdAndTopicName(topicUser.get().getId(), topicName);
        if(topicMetadata.isEmpty())
            throw new TopicNotFoundException("Topic " + topicName + " does not exist!");

        Map<Integer, List<PartitionMessageDto>> partitionToMessageMap = new HashMap<>();
        for(TopicIngestionRequest topicIngestionRequest: topicIngestionRequestList) {

            int partition = PartitionHashGenerator.getPartition(topicIngestionRequest.getMessageKey(), topicMetadata.get().getPartitions());
            if(!partitionToMessageMap.containsKey(partition))
                partitionToMessageMap.put(partition, new ArrayList<>());

            PartitionMessageDto partitionMessageDto = PartitionMessageDto.builder()
                    .msgKey(topicIngestionRequest.getMessageKey())
                    .msgValue(topicIngestionRequest.getMessage())
                    .timestamp(topicIngestionRequest.getTimestamp())
                    .msgHeaders(convertMapToJson(topicIngestionRequest.getHeaders()))
                    .priority(Optional.ofNullable(topicIngestionRequest.getPriority()).orElse(1))
                    .build();

            partitionToMessageMap.get(partition).add(partitionMessageDto);
        }

        List<BasePartitionIngestion> partitionIngestionList = new ArrayList<>();
        for(Integer partition: partitionToMessageMap.keySet()) {

            BasePartitionIngestion partitionIngestion = PartitionIngestionFactory.get(
                    topicMetadata.get().getType(), topicName, partition, partitionToMessageMap.get(partition));

            partitionIngestionList.add(partitionIngestion);
        }

        for(BasePartitionIngestion partitionIngestion: partitionIngestionList) {
            partitionIngestion.ingest(datastoreTemplate, 3);
        }

    }

    public static String convertMapToJson(Map<String, String> headers) {
        if(headers == null)
            return null;

        return "{" +
                headers.entrySet()
                        .stream()
                        .map(e -> "\"" + e.getKey() + "\":\"" + e.getValue() + "\"")
                        .collect(Collectors.joining(",")) +
                "}";
    }


}
