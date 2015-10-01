package org.apache.mesos.elasticsearch.scheduler;

import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.common.Discovery;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.ZonedDateTime;
import java.util.Properties;

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

    public static Task from(Protos.TaskInfo taskInfo, Protos.TaskStatus taskStatus) {
        Properties data = new Properties();
        try {
            data.load(taskInfo.getData().newInput());
        } catch (IOException e) {
            throw new RuntimeException("Failed to parse properties", e);
        }
        String hostName = data.getProperty("hostname", "UNKNOWN");
        String ipAddress = data.getProperty("ipAddress", hostName);
        ZonedDateTime startedAt = ZonedDateTime.parse(data.getProperty("startedAt", ZonedDateTime.now().toString()));
        Protos.TaskState taskState = null;
        if (taskStatus == null) {
            taskState = Protos.TaskState.TASK_STAGING;
        } else {
            taskState = taskStatus.getState();
        }
        return new Task(
                hostName,
                taskInfo.getTaskId().getValue(),
                taskState,
                startedAt,
                new InetSocketAddress(ipAddress, taskInfo.getDiscovery().getPorts().getPorts(Discovery.CLIENT_PORT_INDEX).getNumber()),
                new InetSocketAddress(ipAddress, taskInfo.getDiscovery().getPorts().getPorts(Discovery.TRANSPORT_PORT_INDEX).getNumber())
        );
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
