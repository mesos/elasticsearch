package org.apache.mesos.elasticsearch.statusapi.controllers;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

import static java.util.Arrays.asList;

@RestController
@RequestMapping("/tasks")
public class TasksController {

    @RequestMapping
    public ResponseEntity<List<GetTasksResponse>> getTasks() {
        return new ResponseEntity<>(asList(new GetTasksResponse("hJLXmY_NTrCytiIMbX4_1g", "example4", "1.60", "Time", "inet[/172.18.58.139:9203]", "inet[/172.18.58.139:9203]", "example4.nodes.cluster")), HttpStatus.OK);
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
    }
}
