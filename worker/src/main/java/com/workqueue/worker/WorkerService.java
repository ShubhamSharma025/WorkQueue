package com.workqueue.worker;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.workqueue.common.Metrics;
import com.workqueue.common.Task;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class WorkerService {

    private final Jedis jedis = new Jedis("localhost", 6379);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private static final String QUEUE_NAME = "work_queue";
    private final AtomicInteger jobsDone = new AtomicInteger(0);
    private final AtomicInteger jobsFailed = new AtomicInteger(0);

    @Scheduled(fixedDelay = 5000) // ← FIXED: Use fixedDelay, not fixedRate
    public void processQueue() {
        try {
            // ← Use timeout = 4 seconds (not 0!)
            List<String> result = jedis.brpop(4, QUEUE_NAME);

            if (result == null || result.isEmpty() || result.size() < 2) {
                return; // No job
            }

            String taskJson = result.get(1);
            Task task = objectMapper.readValue(taskJson, Task.class);
            processTask(task);
            jobsDone.incrementAndGet();

        } catch (JsonProcessingException e) {
            System.err.println("JSON parse error: " + e.getMessage());
            jobsFailed.incrementAndGet();
        } catch (JedisConnectionException e) {
            System.err.println("Redis disconnected: " + e.getMessage());
            // Will retry next cycle
        } catch (Exception e) {
            System.err.println("Unexpected error in worker: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void processTask(Task task) {
        System.out.println("Processing task: " + task.getType());
        switch (task.getType()) {
            case "send_email" -> {
                String to = (String) task.getPayload().get("to");
                String subject = (String) task.getPayload().get("subject");
                System.out.println("Sending email to " + to + " with subject " + subject);
            }
            case "resize_image" -> {
                Number x = (Number) task.getPayload().get("new_x");
                Number y = (Number) task.getPayload().get("new_y");
                System.out.println("Resizing image to x: " + x + " y: " + y);
            }
            case "generate_pdf" -> System.out.println("Generating pdf...");
            default -> {
                System.out.println("Unsupported task type: " + task.getType());
                jobsFailed.incrementAndGet();
            }
        }
    }

    public Metrics getMetrics() {
        long totalJobsInQueue = 0;
        try {
            totalJobsInQueue = jedis.llen(QUEUE_NAME);
        } catch (Exception e) {
            System.err.println("Redis error in metrics: " + e.getMessage());
            totalJobsInQueue = -1;
        }
        return new Metrics(totalJobsInQueue, jobsDone.get(), jobsFailed.get());
    }
}