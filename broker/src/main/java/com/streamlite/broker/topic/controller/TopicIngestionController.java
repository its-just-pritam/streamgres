package com.streamlite.broker.topic.controller;

import com.streamlite.broker.topic.dto.TopicIngestionRequest;
import com.streamlite.broker.topic.dto.TopicRequest;
import com.streamlite.broker.topic.exception.TopicConflictException;
import com.streamlite.broker.topic.exception.TopicGenericException;
import com.streamlite.broker.topic.exception.TopicInvalidException;
import com.streamlite.broker.topic.exception.TopicNotFoundException;
import com.streamlite.broker.topic.model.TopicMetadata;
import com.streamlite.broker.topic.service.TopicIngestionService;
import com.streamlite.broker.topic.service.TopicManagerService;
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
@RequestMapping("/user/{tenantId}")
public class TopicIngestionController {

    private final TopicIngestionService topicIngestionService;

    public TopicIngestionController(
            @Autowired
            TopicIngestionService topicIngestionService) {
        this.topicIngestionService = topicIngestionService;
    }

    @GetMapping("/test")
    public ResponseEntity<?> getData(HttpServletRequest request, @PathVariable String tenantId) {
        String client = String.valueOf(request.getAttribute("X-Tenant-Id"));
        if(!Objects.equals(tenantId, client))
            return ResponseEntity.badRequest().build();

        return ResponseEntity.ok(Map.of("client", client, "message", "Authorized!"));
    }

    @PostMapping("/ingest/{topicName}")
    public ResponseEntity<String> ingestMessage(
            @PathVariable String tenantId,
            @PathVariable String topicName,
            @RequestBody List<TopicIngestionRequest> topicIngestionRequestList) {
        for(TopicIngestionRequest topicIngestionRequest: topicIngestionRequestList)
            topicIngestionRequest.sanitize(topicName);

        try {
            topicIngestionService.ingest(tenantId,topicName,topicIngestionRequestList);
            return ResponseEntity.accepted().build();
        } catch (Exception e) {
            if(e instanceof UserNotFoundException)
                throw e;
            else if(e instanceof TopicNotFoundException)
                throw e;
            else
                return ResponseEntity.internalServerError().body(e.getMessage());
        }
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

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<Object> handleError(HttpMessageNotReadableException ex) {
        return ResponseEntity
                .status(HttpStatus.BAD_REQUEST)
                .body(ex.getMessage());
    }

}