package org.apache.mesos.elasticsearch.scheduler;

import java.net.InetSocketAddress;
import java.time.ZonedDateTime;

/**
 * Task on a host.
 */
public class Task {

    private String taskId;

    private String hostname;
    private ZonedDateTime startedAt;
    private InetSocketAddress clientAddress;
    private InetSocketAddress transportAddress;

    public Task(String hostname, String taskId, ZonedDateTime startedAt, InetSocketAddress clientInterface, InetSocketAddress transportAddress) {
        this.hostname = hostname;
        this.taskId = taskId;
        this.startedAt = startedAt;
        this.clientAddress = clientInterface;
        this.transportAddress = transportAddress;
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

    public InetSocketAddress getClientAddress() {
        return clientAddress;
    }

    public InetSocketAddress getTransportAddress() {
        return transportAddress;
    }
}
