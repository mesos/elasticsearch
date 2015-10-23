package org.apache.mesos.elasticsearch.systemtest;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Collections;
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
    private final String tasksEndPoint;
    private HttpResponse<JsonNode> response;
    private int nodesCount;
    private String nodesState;

    public TasksResponse(String schedulerIpAddress, int nodesCount) {
        this(schedulerIpAddress, nodesCount, null);
    }

    public TasksResponse(String schedulerIpAddress, int nodesCount, String nodesState) {
        this.nodesCount = nodesCount;
        this.nodesState = nodesState;
        tasksEndPoint = "http://" + schedulerIpAddress + ":" + TEST_CONFIG.getSchedulerGuiPort() + "/v1/tasks";
        await().atMost(5, TimeUnit.MINUTES).pollInterval(1, TimeUnit.SECONDS).until(new TasksCall());
    }

    class TasksCall implements Callable<Boolean> {
        @Override
        public Boolean call() throws Exception {
            try {
                LOGGER.debug("Fetching tasks on " + tasksEndPoint);
                response = Unirest.get(tasksEndPoint).asJson();
                if (!isExpectedNumberOfNodes()) {
                    return false;
                }
                JSONArray tasks = response.getBody().getArray();
                for (int i = 0; i < tasks.length(); i++) {
                    JSONObject task = tasks.getJSONObject(i);
                    if (!isInState(task)) {
                        return false;
                    }
                    if (!canPingESNode(task)) {
                        return false;
                    }
                }
            } catch (UnirestException e) {
                return false;
            }
            return true;
        }

        private boolean isInState(JSONObject task) {
            if (nodesState != null && !nodesState.isEmpty()) {
                Object state = task.get("state");
                boolean res = state.equals(nodesState);
                LOGGER.debug("Checking that node is in expected state: " + state + ".equals(" + nodesState + " = " + res);
                return res;
            } else {
                return true;
            }
        }

        private boolean canPingESNode(JSONObject task) {
            String url = "http://" + task.getString("http_address");
            LOGGER.debug("Querying ES endpoint: " + url);
            try {
                Unirest.get(url).asJson();
            } catch (UnirestException e) {
                return false;
            }
            return true;
        }

        private boolean isExpectedNumberOfNodes() {
            int numNodes = response.getBody().getArray().length();
            boolean res = numNodes == nodesCount;
            LOGGER.debug("Checking expected number of nodes: " + numNodes + " == " + nodesCount + " = " + res);
            return res;
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
