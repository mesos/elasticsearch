package org.apache.mesos.elasticsearch.scheduler.controllers;

import org.apache.log4j.Logger;
import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.scheduler.Configuration;
import org.apache.mesos.elasticsearch.scheduler.ElasticsearchScheduler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 *
 */
@RestController
@RequestMapping("/cluster")
public class ClusterController {
    public static final Logger LOGGER = Logger.getLogger(ClusterController.class);

    @Autowired
    ElasticsearchScheduler scheduler;

    @Autowired
    Configuration configuration;

    @RequestMapping(method = RequestMethod.GET)
    public ClusterInfoResponse clusterInfo() {
        ClusterInfoResponse response = new ClusterInfoResponse();
        response.name = configuration.getTaskName();
        response.configuration = toMap(configuration);
        return response;
    }

    private Map<String, Object> toMap(Configuration configuration) {
        return Arrays.stream(configuration.getClass().getDeclaredMethods()).filter(this::isGetter).collect(Collectors.toMap(method -> method.getName().substring(3), this::invokeConfigurationGetter));
    }

    private boolean isGetter(Method method) {
        return method.getName().startsWith("get") || method.getName().startsWith("is");
    }

    private Object invokeConfigurationGetter(Method method) {
        try {
            final Object result = method.invoke(configuration);
            if (result instanceof Number || result instanceof Boolean || result instanceof String) {
                return result;
            }
            else if (result instanceof Protos.FrameworkID) {
                return ((Protos.FrameworkID) result).getValue();
            }
            return result.toString();
        } catch (Exception e) {
            LOGGER.warn("Failed to invoce method", e);
            return "--ERROR--";
        }
    }

    /**
     * HTTP response entity class
     */
    public static class ClusterInfoResponse {
        public String name;
        public Map<String, Object> configuration;
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
