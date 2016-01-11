package org.apache.mesos.elasticsearch.systemtest;

import com.containersol.minimesos.mesos.DockerClientFactory;
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
    private final Boolean portsExposed;

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
                String oldAddress = (String) jsonObject.remove("http_address");
                String newAddress = Configuration.getDocker0AdaptorIpAddress(DockerClientFactory.build())
                        + ":" + oldAddress.split(":")[1];
                jsonObject.put("http_address", newAddress);
            }
            tasks.add(jsonObject);
        }
        return tasks;
    }
}
