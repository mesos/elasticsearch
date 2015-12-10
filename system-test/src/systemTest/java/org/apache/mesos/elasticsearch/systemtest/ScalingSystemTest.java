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
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static junit.framework.Assert.assertEquals;

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

    public String getNumberOfNodes(String ipAddress) throws UnirestException {
        return Unirest.get("http://" + ipAddress + ":" + WEBUI_PORT + "/v1/cluster/elasticsearchNodes").asJson().getBody().getObject().get("value").toString();
    }
}
