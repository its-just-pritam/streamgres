package com.streamlite.broker.consumer.service;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.rpc.Status;
import com.streamlite.broker.consumer.dto.ConsumerState;
import com.streamlite.broker.consumer.dto.session.LiveConsumerGroup;
import com.streamlite.broker.consumer.dto.session.LivePartition;
import com.streamlite.broker.consumer.dto.session.LiveTopic;
import com.streamlite.broker.consumer.dto.session.LiveUser;
import com.streamlite.broker.consumer.exception.ConsumerNotFoundException;
import com.streamlite.broker.consumer.model.ConsumerMetadata;
import com.streamlite.broker.grpc.*;
import com.streamlite.broker.topic.exception.TopicNotFoundException;
import com.streamlite.broker.user.exception.UserNotFoundException;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.advice.GrpcAdvice;
import net.devh.boot.grpc.server.advice.GrpcExceptionHandler;
import net.devh.boot.grpc.server.service.GrpcService;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.OffsetDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static com.streamlite.broker.consumer.ConsumerConnections.*;

@Slf4j
@GrpcService
public class ConsumerStreamingServiceImpl extends ConsumerStreamingServiceGrpc.ConsumerStreamingServiceImplBase {

    @Autowired
    private ConsumerService consumerService;

    @Override
    public void join(RpcConsumerMetadata request, StreamObserver<PartitionAssignmentMetadata> responseObserver) {

        List<ConsumerMetadata> consumerMetadataList = consumerService.validateListener(request.getUser(), request.getTopic(), request.getConsumerGroup());
        log.info("Consumer instance joining → group={}, instance={}, topic={}",
                request.getConsumerGroup(), request.getInstanceId(), request.getTopic());

        // Return if consumer is already re-balancing
        String consumerStatusKey = request.getUser() + ";" + request.getTopic() + ";" + request.getConsumerGroup();
        if(consumerStateMap.computeIfAbsent(consumerStatusKey, t -> ConsumerState.IDLE) == ConsumerState.REBALANCING) {
            responseObserver.onNext(PartitionAssignmentMetadata.newBuilder()
                    .setTopic(request.getTopic())
                    .setConsumerGroup(request.getConsumerGroup())
                    .setConsumerStatus(ConsumerState.REBALANCING.name())
                    .build());
            responseObserver.onCompleted();
            return;
        } else consumerStateMap.put(consumerStatusKey, ConsumerState.REBALANCING);

        LiveUser liveUser = activeUsers.computeIfAbsent(request.getUser(), au -> new LiveUser());
        LiveTopic liveTopic = liveUser.getTopics().computeIfAbsent(request.getTopic(), t -> new LiveTopic());
        LiveConsumerGroup liveConsumerGroup = liveTopic.getConsumerGroups().computeIfAbsent(request.getConsumerGroup(), cg -> new LiveConsumerGroup());

        // Time re-balance existing live partition assignments after addition of a new instance
        Map<Integer, LivePartition> newLivePartitions = balancePartitions(request, liveConsumerGroup, consumerMetadataList);
        liveConsumerGroup.setPartitions(newLivePartitions);

        Set<PartitionAssignmentMetadata> partitionAssignments = newLivePartitions.values().stream()
                .filter(livePartition -> Objects.equals(livePartition.getInstanceId(), request.getInstanceId()))
                .map(livePartition -> PartitionAssignmentMetadata.newBuilder()
                        .setTopic(request.getTopic())
                        .setConsumerGroup(request.getConsumerGroup())
                        .setConsumerId(request.getConsumerGroup() + "-" + livePartition.getPartition())
                        .setPartition(livePartition.getPartition())
                        .setSession(livePartition.getSession())
                        .setConsumerStatus(ConsumerState.ACTIVE.name())
                        .build())
                .collect(Collectors.toSet());


        // Push partition assignment to client
        for(PartitionAssignmentMetadata partitionAssignmentMetadata: partitionAssignments) {
            responseObserver.onNext(partitionAssignmentMetadata);
            log.info("Assigned partition={} with session={}", partitionAssignmentMetadata.getPartition(), partitionAssignmentMetadata.getSession());
        }

        responseObserver.onCompleted();
        consumerStateMap.put(consumerStatusKey, ConsumerState.ACTIVE);
    }

    @Override
    public void refresh(RpcConsumerMetadata request, StreamObserver<PartitionAssignmentMetadata> responseObserver) {

        List<ConsumerMetadata> consumerMetadataList = consumerService.validateListener(request.getUser(), request.getTopic(), request.getConsumerGroup());
        log.info("Consumer instance refreshing → group={}, instance={}, topic={}",
                request.getConsumerGroup(), request.getInstanceId(), request.getTopic());

        // Return if consumer is already re-balancing
        String consumerStatusKey = request.getUser() + ";" + request.getTopic() + ";" + request.getConsumerGroup();
        if(consumerStateMap.computeIfAbsent(consumerStatusKey, t -> ConsumerState.IDLE) != ConsumerState.ACTIVE) {
            responseObserver.onNext(PartitionAssignmentMetadata.newBuilder()
                    .setTopic(request.getTopic())
                    .setConsumerGroup(request.getConsumerGroup())
                    .setConsumerStatus(consumerStateMap.get(consumerStatusKey).name())
                    .build());
            responseObserver.onCompleted();
            return;
        }

        LiveUser liveUser = activeUsers.computeIfAbsent(request.getUser(), au -> new LiveUser());
        LiveTopic liveTopic = liveUser.getTopics().computeIfAbsent(request.getTopic(), t -> new LiveTopic());
        LiveConsumerGroup liveConsumerGroup = liveTopic.getConsumerGroups().computeIfAbsent(request.getConsumerGroup(), cg -> new LiveConsumerGroup());

        Set<PartitionAssignmentMetadata> partitionAssignments = liveConsumerGroup.getPartitions().values()
                .stream()
                .filter(livePartition -> Objects.equals(livePartition.getInstanceId(), request.getInstanceId()))
                .map(livePartition -> PartitionAssignmentMetadata.newBuilder()
                        .setTopic(request.getTopic())
                        .setConsumerGroup(request.getConsumerGroup())
                        .setConsumerId(request.getConsumerGroup() + "-" + livePartition.getPartition())
                        .setPartition(livePartition.getPartition())
                        .setSession(livePartition.getSession())
                        .setConsumerStatus(ConsumerState.ACTIVE.name())
                        .build())
                .collect(Collectors.toSet());


        // Push partition assignment to client
        for(PartitionAssignmentMetadata partitionAssignmentMetadata: partitionAssignments) {
            responseObserver.onNext(partitionAssignmentMetadata);
            log.info("Discovered partition={} with session={}", partitionAssignmentMetadata.getPartition(), partitionAssignmentMetadata.getSession());
        }

        responseObserver.onCompleted();
        consumerStateMap.put(consumerStatusKey, ConsumerState.ACTIVE);
    }


    private Map<Integer, LivePartition> balancePartitions(
            RpcConsumerMetadata request,
            LiveConsumerGroup liveConsumerGroup,
            List<ConsumerMetadata> consumerMetadataList) {

        // Collect the existing partition and instance assignment
        List<LivePartition> oldLivePartitions = liveConsumerGroup.getPartitions().values().stream().toList();

        // Collect existing instances from partition assignments
        Set<String> existingInstances = oldLivePartitions.stream()
                .map(LivePartition::getInstanceId)
                .collect(Collectors.toSet());

        // Add new instance to the list of connected instances
        existingInstances.add(request.getInstanceId());

        // Clear all old sessions assignments from existing instances in @activeSessions
        oldLivePartitions.forEach(olp -> activeSessions.remove(olp.getSession()));

        Map<String, Long> existingInstanceToTimeoutMap = existingInstances.stream()
                .collect(Collectors.toMap(ci -> ci, ci -> 0L));
        Map<String, Map<String, Boolean>> existingInstanceToLiveSessionMap = existingInstances.stream()
                .collect(Collectors.toMap(ci -> ci, ci -> new HashMap<>()));

        for (LivePartition oldLivePartition: oldLivePartitions) {

            // Retain timeout values for existing instances in @existingInstanceToTimeoutMap
            existingInstanceToTimeoutMap.put(oldLivePartition.getInstanceId(), Math.max(
                            existingInstanceToTimeoutMap.get(oldLivePartition.getInstanceId()),
                            oldLivePartition.getTimeout()
                    )
            );

            // Retain existing sessions for re-balancing only if their heartbeat is not timed-out
            if(OffsetDateTime.now().isBefore(oldLivePartition.getLastUpdated().plusSeconds(oldLivePartition.getTimeout())))
                existingInstanceToLiveSessionMap.get(oldLivePartition.getInstanceId()).put(oldLivePartition.getSession(), false);
        }

        List<String> connctedInstanceList = existingInstances.stream().toList();
        int totalConnectedInstances = connctedInstanceList.size();
        return consumerMetadataList.stream().map(consumerMetadata -> {

            LivePartition livePartition = new LivePartition();
            livePartition.setUser(request.getUser());
            livePartition.setTopic(request.getTopic());
            livePartition.setConsumer(request.getConsumerGroup());
            livePartition.setPartition(consumerMetadata.getPartitionId());
            livePartition.setOffset(consumerMetadata.getPartitionOffset());

            // Get the next available instance
            String instance = connctedInstanceList.get(consumerMetadata.getPartitionId() % totalConnectedInstances);
            Map<String, Boolean> sessionStatusMap = existingInstanceToLiveSessionMap.get(instance);

            // Find an available session (false = available)
            String session = sessionStatusMap.entrySet().stream()
                    .filter(entry -> Boolean.FALSE.equals(entry.getValue()))
                    .map(Map.Entry::getKey)
                    .findFirst()
                    .orElseGet(() -> UUID.randomUUID().toString());

            sessionStatusMap.put(session, true);

            long timeout = 300L;
            if(Objects.equals(instance, request.getInstanceId()))
                timeout = Math.max(timeout, request.getTimeout());
            else
                timeout = Math.max(timeout, existingInstanceToTimeoutMap.get(instance));

            livePartition.setTimeout(timeout);
            livePartition.setLastUpdated(OffsetDateTime.now());
            livePartition.setInstanceId(instance);
            livePartition.setSession(session);

            activeSessions.put(session, livePartition);
            return livePartition;

        }).collect(Collectors.toMap(LivePartition::getPartition, livePartition -> livePartition));
    }


    @Override
    public StreamObserver<Heartbeat> listen(StreamObserver<StreamgresMessage> responseObserver) {

        return new StreamObserver<>() {

            @Override
            public void onNext(Heartbeat request) {

                String session = request.getSession();
                if (activeSessions.containsKey(session)) {
                    LivePartition livePartition = activeSessions.get(session);
                    List<StreamgresMessage> streamgresMessageList = consumerService
                            .fetchMessages(livePartition.getUser(), livePartition.getTopic(), livePartition.getConsumer(), livePartition.getPartition())
                            .stream()
                            .map(partitionMessageDto -> StreamgresMessage.newBuilder()
                                    .setMessage(ByteString.copyFrom(partitionMessageDto.getMsgValue().getBytes()))
                                    .setKey(partitionMessageDto.getMsgKey())
                                    .setOffset(partitionMessageDto.getOffset())
                                    .setPartition(livePartition.getPartition())
                                    .setTimestamp(Timestamp.newBuilder()
                                            .setSeconds(partitionMessageDto.getTimestamp().toInstant().getEpochSecond())
                                            .setNanos(partitionMessageDto.getTimestamp().getNano())
                                            .build())
                                    .build())
                            .toList();

                    streamgresMessageList.forEach(responseObserver::onNext);
                }
            }

            @Override
            public void onError(Throwable throwable) {
                log.error("listen() stream encountered error: {}", throwable.getMessage(), throwable);
                responseObserver.onError(throwable);
            }

            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
            }
        };
    }

    @Override
    public StreamObserver<Acknowledgement> ack(StreamObserver<HealthStatus> responseObserver) {
        return new StreamObserver<>() {

            @Override
            public void onNext(Acknowledgement request) {
                String session = request.getSession();
                boolean isAlive = activeSessions.containsKey(session);
                log.info("Acknowledgement from session={} → {}", session, isAlive ? "CONNECTED" : "DISCONNECTED");

                if(!isAlive) {
                    responseObserver.onNext(HealthStatus.newBuilder()
                            .setConnected(false)
                            .build());
                    responseObserver.onCompleted();
                    return;
                }

                OffsetDateTime now = OffsetDateTime.now();
                LivePartition livePartition = activeSessions.get(session);

                // Check is partition assignment is in sync with database
                // If not, update the offset anyway, but send negative pulse to prompt refresh
                if(livePartition.getPartition() == request.getPartition()) {
                    livePartition.setLastUpdated(now);
                    livePartition.setOffset(Math.max(livePartition.getOffset(), request.getOffset()));
                    consumerService.updatePartitionOffset(livePartition.getUser(), livePartition.getTopic(), livePartition.getConsumer(), livePartition.getPartition(), livePartition.getOffset(), now, session);

                    responseObserver.onNext(HealthStatus.newBuilder()
                            .setConnected(true)
                            .build());
                    responseObserver.onCompleted();
                } else {
                    consumerService.verifyAndUpdatePartition(livePartition.getUser(), livePartition.getTopic(), livePartition.getConsumer(), request.getPartition(), livePartition.getOffset(), now, session);
                    responseObserver.onNext(HealthStatus.newBuilder()
                            .setConnected(false)
                            .build());
                    responseObserver.onCompleted();
                }
            }

            @Override
            public void onError(Throwable throwable) {
                log.error("ack() stream encountered error: {}", throwable.getMessage(), throwable);
                responseObserver.onError(throwable);
            }

            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
            }
        };
    }

    @Override
    public void pulse(Heartbeat request, StreamObserver<HealthStatus> responseObserver) {

        String session = request.getSession();
        boolean isAlive = activeSessions.containsKey(session);
        log.info("Heartbeat from session={} → {}", session, isAlive ? "CONNECTED" : "DISCONNECTED");

        if(isAlive)
            activeSessions.get(session).setLastUpdated(OffsetDateTime.now());

        HealthStatus status = HealthStatus.newBuilder()
                .setConnected(isAlive)
                .build();

        responseObserver.onNext(status);
        responseObserver.onCompleted();
    }

    @Override
    public void leave(RpcConsumerMetadata request, StreamObserver<HealthStatus> responseObserver) {

        consumerService.validateListener(request.getUser(), request.getTopic(), request.getConsumerGroup());
        log.info("Consumer instance removing → group={}, instance={}, topic={}",
                request.getConsumerGroup(), request.getInstanceId(), request.getTopic());

        String consumerStatusKey = request.getUser() + ";" + request.getTopic() + ";" + request.getConsumerGroup();
        int retries = 3;

        while(consumerStateMap.computeIfAbsent(consumerStatusKey, t -> ConsumerState.IDLE) == ConsumerState.REBALANCING) {
            if(retries == 0)
                throw new RuntimeException("Could not disconnect, consumer=" + request.getConsumerGroup() + " is re-balancing. Please retry after a while");

            try {
                Thread.sleep(10000L);
            } catch (InterruptedException e) {
                log.error(e.getMessage(), e);
            } finally {
                retries = retries - 1;
            }
        }

        HealthStatus status = HealthStatus.newBuilder()
                .setConnected(false)
                .build();
        consumerStateMap.put(consumerStatusKey, ConsumerState.REBALANCING);
        activeSessions.remove(request.getInstanceId());

        // Return if live user not found
        if(!activeUsers.containsKey(request.getUser())) {
            responseObserver.onNext(status);
            responseObserver.onCompleted();
        }
        LiveUser liveUser = activeUsers.get(request.getUser());

        // Return if live topic not found
        if(!liveUser.getTopics().containsKey(request.getTopic())) {
            responseObserver.onNext(status);
            responseObserver.onCompleted();
        }
        LiveTopic liveTopic = liveUser.getTopics().get(request.getTopic());

        // Return if live consumer not found
        if(!liveTopic.getConsumerGroups().containsKey(request.getConsumerGroup())) {
            responseObserver.onNext(status);
            responseObserver.onCompleted();
        }
        LiveConsumerGroup liveConsumerGroup = liveTopic.getConsumerGroups().get(request.getConsumerGroup());

        // Remove all sessions linked to the consumer
        // When one instance leaves, we clear all the instances for ease
        // When the remaining instances feel the dead pulse, they will rejoin
        liveConsumerGroup.getPartitions().values()
                .forEach(livePartition -> activeSessions.remove(livePartition.getSession()));

        // Reset the partitions in @liveConsumerGroup
        liveConsumerGroup.setPartitions(new HashMap<>());

        responseObserver.onNext(status);
        responseObserver.onCompleted();
        consumerStateMap.put(consumerStatusKey, ConsumerState.IDLE);
    }

    @GrpcAdvice
    public static class GlobalGrpcExceptionHandler {

        @GrpcExceptionHandler(UserNotFoundException.class)
        public StatusRuntimeException handleUnknown(UserNotFoundException ex) {
            return StatusProto.toStatusRuntimeException(
                    Status.newBuilder()
                            .setCode(io.grpc.Status.NOT_FOUND.getCode().value())
                            .setMessage(ex.getMessage())
                            .build()
            );
        }

        @GrpcExceptionHandler(TopicNotFoundException.class)
        public StatusRuntimeException handleUnknown(TopicNotFoundException ex) {
            return StatusProto.toStatusRuntimeException(
                    Status.newBuilder()
                            .setCode(io.grpc.Status.NOT_FOUND.getCode().value())
                            .setMessage(ex.getMessage())
                            .build()
            );
        }

        @GrpcExceptionHandler(ConsumerNotFoundException.class)
        public StatusRuntimeException handleUnknown(ConsumerNotFoundException ex) {
            return StatusProto.toStatusRuntimeException(
                    Status.newBuilder()
                            .setCode(io.grpc.Status.NOT_FOUND.getCode().value())
                            .setMessage(ex.getMessage())
                            .build()
            );
        }
    }

}
