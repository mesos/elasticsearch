package org.apache.mesos.elasticsearch.scheduler.controllers;

import org.apache.mesos.elasticsearch.scheduler.ElasticsearchScheduler;
import org.apache.mesos.elasticsearch.scheduler.Task;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

import static java.util.stream.Collectors.toList;

/**
 *
 */
@RestController
@RequestMapping("/tasks")
public class TasksController {

    @Autowired
    ElasticsearchScheduler scheduler;

    @RequestMapping
    public List<GetTasksResponse> getTasks() {
        return scheduler.getTasks().stream().map(GetTasksResponse::from).collect(toList());
    }

    public static class GetTasksResponse {
        public String id, name, version, startedAt, httpAddress, transportAddress, hostname;

        public GetTasksResponse(String id, String name, String version, String startedAt, String httpAddress, String transportAddress, String hostname) {
            this.id = id;
            this.name = name;
            this.version = version;
            this.startedAt = startedAt;
            this.httpAddress = httpAddress;
            this.transportAddress = transportAddress;
            this.hostname = hostname;
        }

        public static GetTasksResponse from(Task task) {
            return new GetTasksResponse(
                    task.getTaskId(),
                    "UNKOWN: name",
                    "UNKOWN: version",
                    "UNKOWN: started at",
                    "UNKOWN: http address",
                    "UNKOWN: transport address",
                    task.getHostname()
            );
        }
    }
}
