package com.workqueue.common;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Metrics {

    @JsonProperty("total_jobs_in_queue")
    private long totalJobsInQueue;

    @JsonProperty("jobs_done")
    private int jobsDone;

    @JsonProperty("jobs_failed")
    private int jobsFailed;

    public Metrics(long totalJobsInQueue, int jobsDone, int jobsFailed) {
        this.totalJobsInQueue = totalJobsInQueue;
        this.jobsDone = jobsDone;
        this.jobsFailed = jobsFailed;
    }

    public long getTotalJobsInQueue() {
        return totalJobsInQueue;
    }

    public void setTotalJobsInQueue(long totalJobsInQueue) {
        this.totalJobsInQueue = totalJobsInQueue;
    }

    public int getJobsDone() {
        return jobsDone;
    }

    public void setJobsDone(int jobsDone) {
        this.jobsDone = jobsDone;
    }

    public int getJobsFailed() {
        return jobsFailed;
    }

    public void setJobsFailed(int jobsFailed) {
        this.jobsFailed = jobsFailed;
    }
} 