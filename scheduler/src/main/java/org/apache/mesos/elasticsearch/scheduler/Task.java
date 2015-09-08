package org.apache.mesos.elasticsearch.scheduler;

import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.common.Discovery;

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

    public static Task from(Protos.TaskInfo taskInfo) {
        String hostName = taskInfo.getData().toStringUtf8();
        ZonedDateTime startedAt = ZonedDateTime.now(); //TODO:
        return new Task(
                hostName,
                taskInfo.getTaskId().getValue(),
                Protos.TaskState.TASK_STAGING, //TODO: Not sure this is the correct state
                startedAt,
                new InetSocketAddress(hostName, taskInfo.getDiscovery().getPorts().getPorts(Discovery.CLIENT_PORT_INDEX).getNumber()),
                new InetSocketAddress(hostName, taskInfo.getDiscovery().getPorts().getPorts(Discovery.TRANSPORT_PORT_INDEX).getNumber())
        );
    }
}
