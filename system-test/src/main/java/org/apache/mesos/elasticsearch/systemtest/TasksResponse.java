package org.apache.mesos.elasticsearch.systemtest;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.log4j.Logger;
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

    private HttpResponse<JsonNode> response;
    private String schedulerIpAddress;
    private int nodesCount;

    public TasksResponse(String schedulerIpAddress, int nodesCount) {
        this.schedulerIpAddress = schedulerIpAddress;
        this.nodesCount = nodesCount;
        await().atMost(60, TimeUnit.SECONDS).until(new TasksCall());
    }

    class TasksCall implements Callable<Boolean> {

        @Override
        public Boolean call() throws Exception {
            try {
                String tasksEndPoint = "http://" + schedulerIpAddress + ":31100/v1/tasks";
                LOGGER.debug("Fetching tasks on " + tasksEndPoint);
                response = Unirest.get(tasksEndPoint).asJson();
                return response.getBody().getArray().length() == nodesCount;
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
