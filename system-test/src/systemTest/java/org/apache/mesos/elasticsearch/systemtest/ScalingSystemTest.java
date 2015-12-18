package org.apache.mesos.elasticsearch.systemtest;

import com.containersol.minimesos.mesos.DockerClientFactory;
import com.github.dockerjava.api.DockerClient;
import com.jayway.awaitility.Awaitility;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.log4j.Logger;
import org.apache.mesos.elasticsearch.systemtest.base.SchedulerTestBase;
import org.apache.mesos.elasticsearch.systemtest.containers.DataPusherContainer;
import org.json.JSONArray;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/**
 * Tests the scaling capabilities
 */
public class ScalingSystemTest extends SchedulerTestBase {
    private static final Logger LOGGER = Logger.getLogger(ScalingSystemTest.class);
    public static final int WEBUI_PORT = 31100;
    private static DockerClient dockerClient = DockerClientFactory.build();

    @Test
    public void shouldScaleDown() throws UnirestException {
        String ipAddress = getScheduler().getIpAddress();
        String oldNumNodes = getNumberOfNodes(ipAddress);
        LOGGER.debug("Number of nodes: " + oldNumNodes);

        String newNumNodes = "2";
        HttpResponse<JsonNode> response = Unirest.put("http://" + ipAddress + ":" + WEBUI_PORT + "/v1/cluster/elasticsearchNodes").header("Content-Type", "application/json").body(newNumNodes).asJson();
        assertEquals(200, response.getStatus());

        Awaitility.await().atMost(60, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).until(() -> {
            String numNodes = getNumberOfNodes(ipAddress);
            LOGGER.debug("Number of nodes: " + numNodes);
            return numNodes.equals(newNumNodes);
        });
        assertEquals(newNumNodes, getNumberOfNodes(ipAddress));
    }

    @Test
    public void shouldScaleUp() throws UnirestException {
        shouldScaleDown();

        String ipAddress = getScheduler().getIpAddress();
        String oldNumNodes = getNumberOfNodes(ipAddress);
        LOGGER.debug("Number of nodes: " + oldNumNodes);

        String newNumNodes = "3";
        HttpResponse<JsonNode> response = Unirest.put("http://" + ipAddress + ":" + WEBUI_PORT + "/v1/cluster/elasticsearchNodes").header("Content-Type", "application/json").body(newNumNodes).asJson();
        assertEquals(200, response.getStatus());

        Awaitility.await().atMost(60, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).until(() -> {
            String numNodes = getNumberOfNodes(ipAddress);
            LOGGER.debug("Number of nodes: " + numNodes);
            return numNodes.equals(newNumNodes);
        });
        assertEquals(newNumNodes, getNumberOfNodes(ipAddress));
    }

    @Test
    public void shouldNotLoseDataWhenScalingDown() throws UnirestException {
        // Make sure we have three nodes
        shouldScaleUp();

        ESTasks esTasks = new ESTasks(TEST_CONFIG, getScheduler().getIpAddress());
        waitForGreen(esTasks);

        List<String> esAddresses = esTasks.getTasks().stream().map(task -> task.getString("http_address")).collect(Collectors.toList());
        LOGGER.info("Addresses: " + esAddresses);

        DataPusherContainer pusher = new DataPusherContainer(clusterArchitecture.dockerClient, esAddresses.get(0));
        CLUSTER.addAndStartContainer(pusher);
        LOGGER.info("Started data push");

        Awaitility.await().atMost(1, TimeUnit.MINUTES).pollDelay(2, TimeUnit.SECONDS).until(() -> {
            JSONArray responseElements;
            for (String httpAddress : esAddresses) {
                try {
                    responseElements = Unirest.get("http://" + httpAddress + "/_count").asJson().getBody().getArray();
                } catch (Exception e) {
                    LOGGER.error("Unirest exception:" + e.getMessage());
                    return false;
                }
                LOGGER.info(responseElements);
                if (9 > responseElements.getJSONObject(0).getInt("count")) {
                    return false;
                }
            }
            return true;
        });

        Integer correctNumberOfDocuments = Unirest.get("http://" + esAddresses.get(0) + "/_count").asJson().getBody().getArray().getJSONObject(0).getInt("count");
        LOGGER.debug("Correct number of documents: " + correctNumberOfDocuments);

        // Scale down to one node
        String ipAddress = getScheduler().getIpAddress();
        String newNumNodes = "1";
        LOGGER.debug("Scaling down to " + newNumNodes);
        HttpResponse<JsonNode> response = Unirest.put("http://" + ipAddress + ":" + WEBUI_PORT + "/v1/cluster/elasticsearchNodes").header("Content-Type", "application/json").body(newNumNodes).asJson();
        LOGGER.debug("Response " + response.getStatus());
        assertEquals(200, response.getStatus());
        Awaitility.await().atMost(60, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).until(() -> {
            String numNodes = getNumberOfNodes(ipAddress);
            LOGGER.debug("Number of nodes: " + numNodes);
            return numNodes.equals(newNumNodes);
        });

        // Refresh ES address list
        List<String> newAddress = esTasks.getTasks().stream().map(task -> task.getString("http_address")).collect(Collectors.toList());
        LOGGER.info("New address: " + newAddress);
        assertEquals(1, newAddress.size());
        waitForGreen(esTasks);

        Awaitility.await().atMost(60, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).until(() -> {
            try {
                Unirest.get("http://" + newAddress.get(0) + "/_count").asJson().getBody().getArray().getJSONObject(0).getInt("count");
                return true;
            } catch (Exception e) {
                return false;
            }
        });
        Integer newNumberOfDocuments = Unirest.get("http://" + newAddress.get(0) + "/_count").asJson().getBody().getArray().getJSONObject(0).getInt("count");

        // Check that the data is still correct
        LOGGER.debug("New number of documents: " + newNumberOfDocuments);
        assertEquals(correctNumberOfDocuments, newNumberOfDocuments);
    }

    public void waitForGreen(ESTasks esTasks) {
        LOGGER.debug("Wating for green.");
        Awaitility.await().atMost(5, TimeUnit.MINUTES).pollDelay(2, TimeUnit.SECONDS).until(() -> {
            try {
                List<String> esAddresses = esTasks.getTasks().stream().map(task -> task.getString("http_address")).collect(Collectors.toList());
                // This may throw a JSONException if we call before the JSON has been generated. Hence, catch exception.
                String body = Unirest.get("http://" + esAddresses.get(0) + "/_cluster/health").asString().getBody();
                LOGGER.debug(body);
                return body.contains("green");
            } catch (Exception e) {
                return false;
            }
        });
    }

    public String getNumberOfNodes(String ipAddress) throws UnirestException {
        return Unirest.get("http://" + ipAddress + ":" + WEBUI_PORT + "/v1/cluster/elasticsearchNodes").asJson().getBody().getObject().get("value").toString();
    }
}
