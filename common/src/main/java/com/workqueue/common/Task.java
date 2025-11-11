package com.workqueue.common;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

public class Task {

    @JsonProperty("type")
    private String type;

    @JsonProperty("payload")
    private Map<String, Object> payload;
    
    @JsonProperty("retries")
    private int retries;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Map<String, Object> getPayload() {
        return payload;
    }

    public void setPayload(Map<String, Object> payload) {
        this.payload = payload;
    }

    public int getRetries() {
        return retries;
    }

    public void setRetries(int retries) {
        this.retries = retries;
    }
} 