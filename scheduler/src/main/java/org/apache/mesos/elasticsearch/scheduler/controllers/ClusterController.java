package org.apache.mesos.elasticsearch.scheduler.controllers;

import org.apache.mesos.elasticsearch.scheduler.ElasticsearchScheduler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
@RestController
@RequestMapping("/cluster")
public class ClusterController {

    @Autowired
    ElasticsearchScheduler scheduler;

    @RequestMapping(method = RequestMethod.GET)
    public ClusterInfoResponse clusterInfo() {
        ClusterInfoResponse response = new ClusterInfoResponse();
        response.name = "TODO";
        response.configuration = Collections.emptyMap();
        return response;
    }

    /**
     * HTTP response entity class
     */
    public static class ClusterInfoResponse {
        public String name;
        public Map<String, String> configuration;
    }

    @RequestMapping(value = "/scheduler", method = RequestMethod.GET)
    public ClusterSchedulerInfoResponse schedulerInfo() {
        final ClusterSchedulerInfoResponse response = new ClusterSchedulerInfoResponse();
        response.docker = dockerMap("image", "tag", "id");
        return response;
    }

    /**
     * HTTP response entity class
     */
    public static class ClusterSchedulerInfoResponse {
        public Map<String, String> docker = new HashMap<>();
    }

    @RequestMapping(value = "/executors", method = RequestMethod.GET)
    public ClusterExecutorsInfoResponse executorsInfo() {
        final ClusterExecutorsInfoResponse response = new ClusterExecutorsInfoResponse();
        response.docker = dockerMap("image", "tag", "id");
        return response;
    }

    /**
     * HTTP response entity class
     */
    public static class ClusterExecutorsInfoResponse {
        public Map<String, String> docker;
    }

    private Map<String, String> dockerMap(String image, String tag, String id) {
        final HashMap<String, String> map = new HashMap<>();
        map.put("image", image);
        map.put("tag", tag);
        map.put("id", id);
        return map;
    }
}
