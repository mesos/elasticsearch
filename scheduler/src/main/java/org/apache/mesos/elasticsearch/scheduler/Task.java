package org.apache.mesos.elasticsearch.scheduler;

import java.time.ZonedDateTime;

/**
 * Task on a host.
 */
public class Task {

    private String taskId;

    private String hostname;
    private ZonedDateTime startedAt;

    public Task(String hostname, String taskId, ZonedDateTime startedAt) {
        this.hostname = hostname;
        this.taskId = taskId;
        this.startedAt = startedAt;
    }

    public String getHostname() {
        return hostname;
    }

    public String getTaskId() {
        return taskId;
    }

    public ZonedDateTime getStartedAt() {
        return startedAt;
    }
}
