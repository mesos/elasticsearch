package org.apache.mesos.elasticsearch.systemtest;

import com.jayway.awaitility.Awaitility;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.log4j.Logger;
import org.apache.mesos.elasticsearch.common.elasticsearch.ElasticsearchParser;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.mesos.elasticsearch.common.elasticsearch.ElasticsearchParser.HTTP_ADDRESS;

/**
 * Get Array of tasks from the API
 */
public class ESTasks {
    private static final Logger LOGGER = Logger.getLogger(ESTasks.class);
    private final String tasksEndPoint;
    private final Boolean portsExposed;
    private String dockerHostAddress = Configuration.getDocker0AdaptorIpAddress();

    public ESTasks(Configuration config, String schedulerIpAddress, Boolean portsExposed) {
        this.portsExposed = portsExposed;
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
            JSONObject jsonObject = response.getBody().getArray().getJSONObject(i);
            // If the ports are exposed on the docker adaptor, then force the http_address's to point to the docker adaptor IP address.
            // This is a nasty hack, much like `if (testing) doSomething();`. This means we are no longer testing a
            // real-life network setup.
            if (portsExposed) {
                String oldAddress = (String) jsonObject.remove(HTTP_ADDRESS);
                String newAddress = dockerHostAddress
                        + ":" + oldAddress.split(":")[1];
                jsonObject.put(HTTP_ADDRESS, newAddress);
            }
            tasks.add(jsonObject);
        }
        return tasks;
    }

    // TODO (pnw): I shouldn't have to prepend http everywhere. Add here instead.
    public List<String> getEsHttpAddressList() throws UnirestException {
        return getTasks().stream().map(ElasticsearchParser::parseHttpAddress).collect(Collectors.toList());
    }

    public void waitForGreen(Integer numNodes) {
        LOGGER.debug("Wating for green and " + numNodes + " nodes.");
        Awaitility.await().atMost(5, TimeUnit.MINUTES).pollInterval(1, TimeUnit.SECONDS).until(() -> { // This can take some time, somtimes.
            try {
                List<String> esAddresses = getEsHttpAddressList();
                // This may throw a JSONException if we call before the JSON has been generated. Hence, catch exception.
                final JSONObject body = Unirest.get("http://" + esAddresses.get(0) + "/_cluster/health").asJson().getBody().getObject();
                final boolean numberOfNodes = body.getInt("number_of_nodes") == numNodes;
                final boolean green = body.getString("status").equals("green");
                LOGGER.debug(green + " and " + numberOfNodes + ": " + body);
                return green && numberOfNodes;
            } catch (Exception e) {
                LOGGER.debug(e);
                return false;
            }
        });
    }

    public Integer getDocumentCount(String httpAddress) throws UnirestException {
        JSONArray responseElements = Unirest.get("http://" + httpAddress + "/_count").asJson().getBody().getArray();
        LOGGER.info(responseElements);
        return responseElements.getJSONObject(0).getInt("count");
    }

    public void waitForCorrectDocumentCount(Integer docCount) throws UnirestException {
        List<String> esAddresses = getEsHttpAddressList();
        Awaitility.await().atMost(1, TimeUnit.MINUTES).pollDelay(2, TimeUnit.SECONDS).until(() -> {
            for (String httpAddress : esAddresses) {
                try {
                    Integer count = getDocumentCount(httpAddress);
                    if (count == 0 || count % docCount != 0) { // This allows for repeated testing.
                        return false;
                    }
                } catch (Exception e) {
                    LOGGER.error("Unirest exception:" + e.getMessage());
                    return false;
                }
            }
            return true;
        });
    }
}
