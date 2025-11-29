package com.streamlite.broker.topic.controller;

import com.streamlite.broker.topic.dto.TopicRequest;
import com.streamlite.broker.topic.exception.TopicConflictException;
import com.streamlite.broker.topic.exception.TopicGenericException;
import com.streamlite.broker.topic.exception.TopicInvalidException;
import com.streamlite.broker.topic.exception.TopicNotFoundException;
import com.streamlite.broker.topic.model.TopicMetadata;
import com.streamlite.broker.topic.service.TopicManagerService;
import com.streamlite.broker.user.exception.UserNotFoundException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/user/{tenantId}/topic")
public class TopicManagerController {

    private final TopicManagerService topicManagerService;

    public TopicManagerController(
            @Autowired
            TopicManagerService topicManagerService) {
        this.topicManagerService = topicManagerService;
    }

    @GetMapping("/{topicName}")
    public ResponseEntity<TopicMetadata> getTopic(@PathVariable String tenantId, @PathVariable String topicName) {
        TopicMetadata topicMetadata = topicManagerService.getTopic(topicName,tenantId);
        return ResponseEntity.ok(topicMetadata);
    }

    @GetMapping("/list")
    public ResponseEntity<List<TopicMetadata>> listTopic(@PathVariable String tenantId) {
        List<TopicMetadata> topicMetadataList = topicManagerService.listTopics(tenantId);
        return ResponseEntity.ok(topicMetadataList);
    }

    @PostMapping("/{topicName}")
    public ResponseEntity<String> createTopic(@PathVariable String tenantId, @PathVariable String topicName, @RequestBody TopicRequest topicRequest) {
        topicRequest.sanitize();
        topicManagerService.createTopic(topicName,topicRequest,tenantId);
        return ResponseEntity.ok("Topic " + topicName + " created successfully!");

    }

    @DeleteMapping("/{topicName}")
    public ResponseEntity<String> deleteTopic(
            @PathVariable String tenantId,
            @PathVariable String topicName,
            @RequestParam(required = false, defaultValue = "false") Boolean hard) {
        if(hard)
            topicManagerService.hardDeleteTopic(topicName,tenantId);
        else
            topicManagerService.deleteTopic(topicName,tenantId);
        return ResponseEntity.ok("Topic " + topicName + " deleted successfully!");

    }

    @ExceptionHandler(TopicNotFoundException.class)
    public ResponseEntity<Object> handleError(TopicNotFoundException ex) {
        return ResponseEntity
                .status(HttpStatus.NOT_FOUND)
                .body(ex.getMessage());
    }

    @ExceptionHandler(TopicInvalidException.class)
    public ResponseEntity<Object> handleError(TopicInvalidException ex) {
        return ResponseEntity
                .status(HttpStatus.BAD_REQUEST)
                .body(ex.getMessage());
    }

    @ExceptionHandler(TopicGenericException.class)
    public ResponseEntity<Object> handleError(TopicGenericException ex) {
        return ResponseEntity
                .status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(ex.getMessage());
    }

    @ExceptionHandler(TopicConflictException.class)
    public ResponseEntity<Object> handleError(TopicConflictException ex) {
        return ResponseEntity
                .status(HttpStatus.BAD_REQUEST)
                .body(ex.getMessage());
    }

    @ExceptionHandler(UserNotFoundException.class)
    public ResponseEntity<Object> handleError(UserNotFoundException ex) {
        return ResponseEntity
                .status(HttpStatus.NOT_FOUND)
                .body(ex.getMessage());
    }

}