package com.workqueue.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.workqueue.common.Task;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import redis.clients.jedis.Jedis;

@RestController
public class ProducerController {

    private final Jedis jedis = new Jedis("localhost", 6379);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private static final String QUEUE_NAME = "work_queue";


    @PostMapping("/enqueue")
    public ResponseEntity<String> enqueueTask(@RequestBody Task task) {
        try {
            String taskJson = objectMapper.writeValueAsString(task);
            jedis.lpush(QUEUE_NAME, taskJson);
            return ResponseEntity.ok("Task enqueued to redis successfully: " + task.getType());
        } catch (JsonProcessingException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Failed to serialize task");
        }
    }
} 