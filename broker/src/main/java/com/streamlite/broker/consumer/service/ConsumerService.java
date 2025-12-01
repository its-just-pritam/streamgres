package com.streamlite.broker.consumer.service;

import com.streamlite.broker.consumer.dto.ConsumerResponseDto;
import com.streamlite.broker.consumer.dto.PartitionSpecDto;
import com.streamlite.broker.consumer.exception.ConsumerConflictException;
import com.streamlite.broker.consumer.exception.ConsumerNotFoundException;
import com.streamlite.broker.consumer.model.ConsumerMetadata;
import com.streamlite.broker.consumer.repository.ConsumerMetadataRepository;
import com.streamlite.broker.grpc.StreamgresMessage;
import com.streamlite.broker.partition.dto.PartitionMessageDto;
import com.streamlite.broker.partition.reading.*;
import com.streamlite.broker.topic.exception.TopicNotFoundException;
import com.streamlite.broker.topic.model.TopicMetadata;
import com.streamlite.broker.topic.repository.TopicMetadataRepository;
import com.streamlite.broker.user.exception.UserNotFoundException;
import com.streamlite.broker.user.model.TopicUser;
import com.streamlite.broker.user.repository.TopicUserRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
public class ConsumerService {

    private final JdbcTemplate jdbcTemplate;
    private final TopicUserRepository topicUserRepository;
    private final TopicMetadataRepository topicMetadataRepository;
    private final ConsumerMetadataRepository consumerMetadataRepository;

    public ConsumerService(JdbcTemplate jdbcTemplate, TopicUserRepository topicUserRepository, TopicMetadataRepository topicMetadataRepository, ConsumerMetadataRepository consumerMetadataRepository) {
        this.jdbcTemplate = jdbcTemplate;
        this.topicUserRepository = topicUserRepository;
        this.topicMetadataRepository = topicMetadataRepository;
        this.consumerMetadataRepository = consumerMetadataRepository;
    }


    public void add(String tenantId, String topicName, String consumerName) {

        Optional<TopicUser> topicUser = topicUserRepository.findByTenantId(tenantId);
        if(topicUser.isEmpty())
            throw new UserNotFoundException("User " + tenantId + " not found!");

        Optional<TopicMetadata> topicMetadata = topicMetadataRepository.findByUserIdAndTopicName(topicUser.get().getId(), topicName);
        if(topicMetadata.isEmpty())
            throw new TopicNotFoundException("Topic " + topicName + " does not exist!");

        List<ConsumerMetadata> consumerMetadataList = consumerMetadataRepository.findByUserIdAndTopicIdAndName(
                topicUser.get().getId(), topicMetadata.get().getId(), consumerName);
        if(!consumerMetadataList.isEmpty())
            throw new ConsumerConflictException("Consumer " + consumerName + " already exists!");

        for(int partition=0; partition<topicMetadata.get().getPartitions(); ++partition) {
            consumerMetadataList.add(
                    ConsumerMetadata.builder()
                    .topicId(topicMetadata.get().getId())
                    .userId(topicUser.get().getId())
                    .name(consumerName)
                    .partitionId(partition)
                    .partitionOffset(0L)
                    .build()
            );
        }

        consumerMetadataRepository.saveAll(consumerMetadataList);
    }

    public void delete(String tenantId, String topicName, String consumerName) {

        Optional<TopicUser> topicUser = topicUserRepository.findByTenantId(tenantId);
        if(topicUser.isEmpty())
            throw new UserNotFoundException("User " + tenantId + " not found!");

        Optional<TopicMetadata> topicMetadata = topicMetadataRepository.findByUserIdAndTopicName(topicUser.get().getId(), topicName);
        if(topicMetadata.isEmpty())
            throw new TopicNotFoundException("Topic " + topicName + " does not exist!");

        List<ConsumerMetadata> consumerMetadataList = consumerMetadataRepository.findByUserIdAndTopicIdAndName(
                topicUser.get().getId(), topicMetadata.get().getId(), consumerName);
        if(consumerMetadataList.isEmpty())
            throw new ConsumerNotFoundException("Consumer " + consumerName + " does not exist!");

        List<UUID> consumerIds = consumerMetadataList.stream().map(ConsumerMetadata::getId).toList();
        consumerMetadataRepository.deleteAllById(consumerIds);

    }

    public ConsumerResponseDto get(String tenantId, String topicName, String consumerName) {

        Optional<TopicUser> topicUser = topicUserRepository.findByTenantId(tenantId);
        if(topicUser.isEmpty())
            throw new UserNotFoundException("User " + tenantId + " not found!");

        Optional<TopicMetadata> topicMetadata = topicMetadataRepository.findByUserIdAndTopicName(topicUser.get().getId(), topicName);
        if(topicMetadata.isEmpty())
            throw new TopicNotFoundException("Topic " + topicName + " does not exist!");

        List<ConsumerMetadata> consumerMetadataList = consumerMetadataRepository.findByUserIdAndTopicIdAndName(
                topicUser.get().getId(), topicMetadata.get().getId(), consumerName);
        if(consumerMetadataList.isEmpty())
            throw new ConsumerNotFoundException("Consumer " + consumerName + " does not exist!");

        ConsumerResponseDto consumerResponseDto = new ConsumerResponseDto();
        consumerResponseDto.setName(consumerName);
        consumerResponseDto.setTopic(topicName);
        consumerResponseDto.setPartitionSpec(new ArrayList<>());

        for (ConsumerMetadata consumerMetadata: consumerMetadataList) {
            PartitionSpecDto partitionSpecDto = new PartitionSpecDto();
            partitionSpecDto.setId(consumerMetadata.getPartitionId());
            partitionSpecDto.setOffset(consumerMetadata.getPartitionOffset());
            partitionSpecDto.setCreatedAt(consumerMetadata.getCreatedAt());
            partitionSpecDto.setUpdatedAt(consumerMetadata.getUpdatedAt());
            consumerResponseDto.getPartitionSpec().add(partitionSpecDto);
        }

        return consumerResponseDto;
    }

    public List<ConsumerResponseDto> list(String tenantId, String topicName) {

        Optional<TopicUser> topicUser = topicUserRepository.findByTenantId(tenantId);
        if(topicUser.isEmpty())
            throw new UserNotFoundException("User " + tenantId + " not found!");

        Optional<TopicMetadata> topicMetadata = topicMetadataRepository.findByUserIdAndTopicName(topicUser.get().getId(), topicName);
        if(topicMetadata.isEmpty())
            throw new TopicNotFoundException("Topic " + topicName + " does not exist!");

        List<ConsumerMetadata> consumerMetadataList = consumerMetadataRepository.findByUserIdAndTopicId(
                topicUser.get().getId(), topicMetadata.get().getId());

        Map<String, List<ConsumerMetadata>> consumerMap = consumerMetadataList.stream()
                .collect(Collectors.groupingBy(ConsumerMetadata::getName));

        List<ConsumerResponseDto> consumerResponseDtoList = new ArrayList<>();
        for (String consumerName: consumerMap.keySet()) {
            ConsumerResponseDto consumerResponseDto = new ConsumerResponseDto();
            consumerResponseDto.setName(consumerName);
            consumerResponseDto.setTopic(topicName);
            consumerResponseDto.setPartitionSpec(new ArrayList<>());

            for (ConsumerMetadata consumerMetadata: consumerMap.get(consumerName)) {
                PartitionSpecDto partitionSpecDto = new PartitionSpecDto();
                partitionSpecDto.setId(consumerMetadata.getPartitionId());
                partitionSpecDto.setOffset(consumerMetadata.getPartitionOffset());
                partitionSpecDto.setCreatedAt(consumerMetadata.getCreatedAt());
                partitionSpecDto.setUpdatedAt(consumerMetadata.getUpdatedAt());
                consumerResponseDto.getPartitionSpec().add(partitionSpecDto);
            }
            consumerResponseDtoList.add(consumerResponseDto);
        }

        return consumerResponseDtoList;
    }

    public List<ConsumerMetadata> validateListener(String tenantId, String topicName, String consumerName) {

        Optional<TopicUser> topicUser = topicUserRepository.findByTenantId(tenantId);
        if(topicUser.isEmpty())
            throw new UserNotFoundException("User " + tenantId + " not found!");

        Optional<TopicMetadata> topicMetadata = topicMetadataRepository.findByUserIdAndTopicName(topicUser.get().getId(), topicName);
        if(topicMetadata.isEmpty())
            throw new TopicNotFoundException("Topic " + topicName + " does not exist!");

        List<ConsumerMetadata> consumerMetadataList = consumerMetadataRepository.findByUserIdAndTopicIdAndName(
                topicUser.get().getId(), topicMetadata.get().getId(), consumerName);
        if(consumerMetadataList.isEmpty())
            throw new ConsumerNotFoundException("Consumer " + consumerName + " does not exist!");

        return consumerMetadataList;
    }

    public void updatePartitionOffset(
            String tenantId,
            String topicName,
            String consumerName,
            Integer partition,
            Long offset,
            OffsetDateTime now,
            String session) {

        Optional<TopicUser> topicUser = topicUserRepository.findByTenantId(tenantId);
        if(topicUser.isEmpty())
            throw new UserNotFoundException("User " + tenantId + " not found!");

        Optional<TopicMetadata> topicMetadata = topicMetadataRepository.findByUserIdAndTopicName(topicUser.get().getId(), topicName);
        if(topicMetadata.isEmpty())
            throw new TopicNotFoundException("Topic " + topicName + " does not exist!");

        Optional<ConsumerMetadata> consumerMetadata = consumerMetadataRepository.findByUserIdAndTopicIdAndNameAndPartitionId(
                topicUser.get().getId(), topicMetadata.get().getId(), consumerName, partition);
        if(consumerMetadata.isEmpty())
            throw new ConsumerNotFoundException("Consumer " + consumerName + " does not exist!");

        consumerMetadata.get().setPartitionOffset(Math.max(consumerMetadata.get().getPartitionOffset(), offset));
        consumerMetadata.get().setUpdatedAt(now);
        consumerMetadata.get().setLeaderBroker(session);
        consumerMetadataRepository.save(consumerMetadata.get());

    }

    public void verifyAndUpdatePartition(
            String tenantId,
            String topicName,
            String consumerName,
            Integer partition,
            Long offset,
            OffsetDateTime now,
            String session) {

        Optional<TopicUser> topicUser = topicUserRepository.findByTenantId(tenantId);
        if(topicUser.isEmpty())
            throw new UserNotFoundException("User " + tenantId + " not found!");

        Optional<TopicMetadata> topicMetadata = topicMetadataRepository.findByUserIdAndTopicName(topicUser.get().getId(), topicName);
        if(topicMetadata.isEmpty())
            throw new TopicNotFoundException("Topic " + topicName + " does not exist!");

        Optional<ConsumerMetadata> consumerMetadata = consumerMetadataRepository.findByUserIdAndTopicIdAndNameAndPartitionId(
                topicUser.get().getId(), topicMetadata.get().getId(), consumerName, partition);
        if(consumerMetadata.isEmpty())
            throw new ConsumerNotFoundException("Consumer " + consumerName + " does not exist!");

        if(Objects.equals(consumerMetadata.get().getLeaderBroker(), session)) {
            consumerMetadata.get().setPartitionOffset(Math.max(consumerMetadata.get().getPartitionOffset(), offset));
            consumerMetadata.get().setUpdatedAt(now);
            consumerMetadata.get().setLeaderBroker(null);
            consumerMetadataRepository.save(consumerMetadata.get());
        }

    }

    public List<PartitionMessageDto> fetchMessages(
            String tenantId,
            String topicName,
            String consumerName,
            Integer partition) {

        Optional<TopicUser> topicUser = topicUserRepository.findByTenantId(tenantId);
        if(topicUser.isEmpty())
            throw new UserNotFoundException("User " + tenantId + " not found!");

        Optional<TopicMetadata> topicMetadata = topicMetadataRepository.findByUserIdAndTopicName(topicUser.get().getId(), topicName);
        if(topicMetadata.isEmpty())
            throw new TopicNotFoundException("Topic " + topicName + " does not exist!");

        Optional<ConsumerMetadata> consumerMetadata = consumerMetadataRepository.findByUserIdAndTopicIdAndNameAndPartitionId(
                topicUser.get().getId(), topicMetadata.get().getId(), consumerName, partition);
        if(consumerMetadata.isEmpty())
            throw new ConsumerNotFoundException("Consumer " + consumerName + " does not exist!");

        Long offset = consumerMetadata.get().getPartitionOffset();
        String type = topicMetadata.get().getType();

        BasePartitionReader partitionReader = new NullPartitionReader();
        if(Objects.equals(type, "classic"))
            partitionReader = new ClassicPartitionReader(topicName, partition, offset, 10);
//        else if(Objects.equals(type, "distinct"))
//            partitionReader = new DistinctPartitionReader(topicName, partition, offset, 10);
//        else if(Objects.equals(type, "priority"))
//            partitionReader = new PriorityPartitionReader(topicName, partition, offset, 10);

        return partitionReader.read(jdbcTemplate);
    }
}
