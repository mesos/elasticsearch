package org.apache.mesos.elasticsearch.systemtest;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static com.jayway.awaitility.Awaitility.await;

/**
 * Response which waits until tasks endpoint is ready
 */
public class TasksResponse {

    public static final Logger LOGGER = Logger.getLogger(TasksResponse.class);

    private static final Configuration TEST_CONFIG = new Configuration();
    private HttpResponse<JsonNode> response;
    private String schedulerIpAddress;
    private int nodesCount;
    private String nodesState;

    public TasksResponse(String schedulerIpAddress, int nodesCount) {
        this.schedulerIpAddress = schedulerIpAddress;
        this.nodesCount = nodesCount;
        await().atMost(60, TimeUnit.SECONDS).until(new TasksCall());
    }

    public TasksResponse(String schedulerIpAddress, int nodesCount, String nodesState) {
        this.schedulerIpAddress = schedulerIpAddress;
        this.nodesCount = nodesCount;
        this.nodesState = nodesState;
        await().atMost(60, TimeUnit.SECONDS).until(new TasksCall());
    }

    class TasksCall implements Callable<Boolean> {
        @Override
        public Boolean call() throws Exception {
            try {
                String tasksEndPoint = "http://" + schedulerIpAddress + ":" + TEST_CONFIG.getSchedulerGuiPort() + "/v1/tasks";
                LOGGER.debug("Fetching tasks on " + tasksEndPoint);
                response = Unirest.get(tasksEndPoint).asJson();
                if (nodesState == null || nodesState.isEmpty()) {
                    return response.getBody().getArray().length() == nodesCount;
                } else {
                    if (response.getBody().getArray().length() == nodesCount) {
                        JSONArray tasks = response.getBody().getArray();
                        for (int i = 0; i < tasks.length(); i++) {
                            JSONObject task = tasks.getJSONObject(i);
                            if (!task.get("state").equals(nodesState)) {
                                LOGGER.debug("Waiting until nodes are running...");
                                return false;
                            }
                        }
                        return true;
                    }
                    return false;
                }
            } catch (UnirestException e) {
                LOGGER.debug("Waiting until " + nodesCount + " tasks are started...");
                return false;
            }
        }
    }

    public HttpResponse<JsonNode> getJson() {
        return response;
    }

    public List<JSONObject> getTasks() {
        List<JSONObject> tasks = new ArrayList<>();
        for (int i = 0; i < response.getBody().getArray().length(); i++) {
            tasks.add(response.getBody().getArray().getJSONObject(i));
        }
        return tasks;
    }
}
