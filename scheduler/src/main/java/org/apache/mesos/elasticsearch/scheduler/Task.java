package org.apache.mesos.elasticsearch.scheduler;

/**
 * Task on a host.
 */
public class Task {

    private String taskId;

    private String hostname;

    public Task(String hostname, String taskId) {
        this.hostname = hostname;
        this.taskId = taskId;
    }

    public String getHostname() {
        return hostname;
    }

    public String getTaskId() {
        return taskId;
    }
}
