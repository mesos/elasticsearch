package org.apache.mesos.elasticsearch.systemtest;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 * Get Array of tasks from the API
 */
public class ESTasks {
    private static final Logger LOGGER = Logger.getLogger(ESTasks.class);
    private final String tasksEndPoint;

    public ESTasks(Configuration config, String schedulerIpAddress) {
        tasksEndPoint = "http://" + schedulerIpAddress + ":" + config.getSchedulerGuiPort() + "/v1/tasks";
    }

    public HttpResponse<JsonNode> getResponse() throws UnirestException {
        return Unirest.get(tasksEndPoint).asJson();
    }

    public List<JSONObject> getTasks() throws UnirestException {
        List<JSONObject> tasks = new ArrayList<>();
        LOGGER.debug("Fetching tasks on " + tasksEndPoint);
        HttpResponse<JsonNode> response = Unirest.get(tasksEndPoint).asJson();
        for (int i = 0; i < response.getBody().getArray().length(); i++) {
            tasks.add(response.getBody().getArray().getJSONObject(i));
        }
        return tasks;
    }
}
