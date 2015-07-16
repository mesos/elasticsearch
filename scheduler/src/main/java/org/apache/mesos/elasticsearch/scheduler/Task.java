package org.apache.mesos.elasticsearch.scheduler;

import org.apache.mesos.Protos;

import java.net.InetSocketAddress;
import java.time.ZonedDateTime;

/**
 * Task on a host.
 */
public class Task {

    private String taskId;

    private Protos.TaskState state;

    private String hostname;
    private ZonedDateTime startedAt;
    private InetSocketAddress clientAddress;
    private InetSocketAddress transportAddress;
    public Task(String hostname, String taskId, Protos.TaskState state, ZonedDateTime startedAt, InetSocketAddress clientInterface, InetSocketAddress transportAddress) {
        this.hostname = hostname;
        this.taskId = taskId;
        this.state = state;
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

    public Protos.TaskState getState() {
        return state;
    }

    public void setState(Protos.TaskState state) {
        this.state = state;
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
