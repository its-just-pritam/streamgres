package com.streamlite.broker.consumer.controller;

import com.streamlite.broker.consumer.dto.ConsumerResponseDto;
import com.streamlite.broker.consumer.exception.ConsumerConflictException;
import com.streamlite.broker.consumer.exception.ConsumerNotFoundException;
import com.streamlite.broker.consumer.service.ConsumerService;
import com.streamlite.broker.topic.dto.TopicIngestionRequest;
import com.streamlite.broker.topic.exception.TopicNotFoundException;
import com.streamlite.broker.topic.service.TopicIngestionService;
import com.streamlite.broker.user.exception.UserNotFoundException;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.Objects;

@RestController
@RequestMapping("/user/{tenantId}/topic/{topicName}/consumer")
public class ConsumerManagerController {

    private final ConsumerService consumerService;

    public ConsumerManagerController(
            @Autowired
            ConsumerService consumerService) {
        this.consumerService = consumerService;
    }

    @PostMapping("/{consumerName}")
    public ResponseEntity<String> addConsumer(
            @PathVariable String tenantId,
            @PathVariable String topicName,
            @PathVariable String consumerName) {

        consumerService.add(tenantId, topicName, consumerName);
        return ResponseEntity.ok("Consumer " + consumerName + " created successfully!");
    }

    @DeleteMapping("/{consumerName}")
    public ResponseEntity<String> deleteConsumer(
            @PathVariable String tenantId,
            @PathVariable String topicName,
            @PathVariable String consumerName) {

        consumerService.delete(tenantId, topicName, consumerName);
        return ResponseEntity.ok("Consumer " + consumerName + " deleted successfully!");
    }

    @GetMapping("/{consumerName}")
    public ResponseEntity<ConsumerResponseDto> getConsumer(
            @PathVariable String tenantId,
            @PathVariable String topicName,
            @PathVariable String consumerName) {

        ConsumerResponseDto consumerResponseDto = consumerService.get(tenantId, topicName, consumerName);
        return ResponseEntity.ok(consumerResponseDto);
    }

    @GetMapping("/list")
    public ResponseEntity<List<ConsumerResponseDto>> listConsumers(
            @PathVariable String tenantId,
            @PathVariable String topicName) {

        List<ConsumerResponseDto> consumerResponseDtoList = consumerService.list(tenantId, topicName);
        return ResponseEntity.ok(consumerResponseDtoList);
    }

    @ExceptionHandler(TopicNotFoundException.class)
    public ResponseEntity<Object> handleError(TopicNotFoundException ex) {
        return ResponseEntity
                .status(HttpStatus.NOT_FOUND)
                .body(ex.getMessage());
    }

    @ExceptionHandler(UserNotFoundException.class)
    public ResponseEntity<Object> handleError(UserNotFoundException ex) {
        return ResponseEntity
                .status(HttpStatus.NOT_FOUND)
                .body(ex.getMessage());
    }

    @ExceptionHandler(ConsumerConflictException.class)
    public ResponseEntity<Object> handleError(ConsumerConflictException ex) {
        return ResponseEntity
                .status(HttpStatus.BAD_REQUEST)
                .body(ex.getMessage());
    }

    @ExceptionHandler(ConsumerNotFoundException.class)
    public ResponseEntity<Object> handleError(ConsumerNotFoundException ex) {
        return ResponseEntity
                .status(HttpStatus.NOT_FOUND)
                .body(ex.getMessage());
    }

}