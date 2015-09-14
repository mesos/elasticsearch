package org.apache.mesos.elasticsearch.scheduler.controllers;

import org.apache.mesos.elasticsearch.scheduler.Configuration;
import org.apache.mesos.elasticsearch.scheduler.ElasticsearchScheduler;
import org.apache.mesos.elasticsearch.scheduler.Task;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.InetSocketAddress;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static java.util.stream.Collectors.toList;

/**
 *
 */
@RestController
@RequestMapping("/v1/tasks")
public class TasksController {

    @SuppressWarnings("SpringJavaAutowiringInspection")
    @Autowired
    ElasticsearchScheduler scheduler;

    @SuppressWarnings("SpringJavaAutowiringInspection")
    @Autowired
    Configuration configuration;

    @RequestMapping
    public List<GetTasksResponse> getTasks() {
        return scheduler.getTasks().stream().map(this::from).collect(toList());
    }

    private GetTasksResponse from(Task task) {
        return new GetTasksResponse(
            task.getTaskId(),
            task.getState().toString(),
            configuration.getTaskName(),
            configuration.getVersion(),
            task.getStartedAt().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME),
            toFormattedAddress(task.getClientAddress()),
            toFormattedAddress(task.getTransportAddress()),
            task.getHostname()
        );
    }

    private String toFormattedAddress(InetSocketAddress clientAddress) {
        return String.format("%s:%s", clientAddress.getAddress().getHostAddress(), clientAddress.getPort());
    }

    /**
     *
     */
    public static class GetTasksResponse {
        public String id, state, name, version, startedAt, httpAddress, transportAddress, hostname;

        public GetTasksResponse(String id, String state, String name, String version, String startedAt, String httpAddress, String transportAddress, String hostname) {
            this.id = id;
            this.state = state;
            this.name = name;
            this.version = version;
            this.startedAt = startedAt;
            this.httpAddress = httpAddress;
            this.transportAddress = transportAddress;
            this.hostname = hostname;
        }
    }
}
