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
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * Tests the scaling capabilities. To run multiple times, uncomment the code below.
 */
//@RunWith(Parameterized.class)
public class ScalingSystemTest extends SchedulerTestBase {
    private static final Logger LOGGER = Logger.getLogger(ScalingSystemTest.class);
    public static final int WEBUI_PORT = 31100;
    private static DockerClient dockerClient = DockerClientFactory.build();
    private String ipAddress;
    private ESTasks esTasks;

//    @Parameterized.Parameters
//    public static List<Object[]> data() {
//        return Arrays.asList(new Object[10][0]);
//    }

    @Before
    public void before() {
        ipAddress = getScheduler().getIpAddress();
        esTasks = new ESTasks(TEST_CONFIG, ipAddress, true);
    }

    @Test
    public void shouldScaleDown() throws UnirestException {
        scaleNumNodesTo(ipAddress, 3); // Reset to 3 nodes
        esTasks.waitForGreen();
        LOGGER.debug("Number of nodes: " + getNumberOfNodes(ipAddress));
        scaleNumNodesTo(ipAddress, 2);
        esTasks.waitForGreen();
    }

    @Test
    public void shouldScaleUp() throws UnirestException {
        scaleNumNodesTo(ipAddress, 2); // Reset to 2 nodes
        esTasks.waitForGreen();
        LOGGER.debug("Number of nodes: " + getNumberOfNodes(ipAddress));
        scaleNumNodesTo(ipAddress, 3);
        esTasks.waitForGreen();
    }

    @Test
    public void shouldNotLoseDataWhenScalingDown() throws UnirestException {
        // Make sure we have three nodes
        scaleNumNodesTo(ipAddress, 3);
        esTasks.waitForGreen();

        List<String> esAddresses = esTasks.getEsHttpAddressList();
        LOGGER.info("Addresses: " + esAddresses);

        DataPusherContainer pusher = new DataPusherContainer(CLUSTER_ARCHITECTURE.dockerClient, esAddresses.get(0));
        CLUSTER.addAndStartContainer(pusher, TEST_CONFIG.getClusterTimeout());
        LOGGER.info("Started data push");

        esTasks.waitForCorrectDocumentCount(DataPusherContainer.CORRECT_NUM_DOCS);

        // Scale down to one node
        scaleNumNodesTo(ipAddress, 1);
        esTasks.waitForGreen();

        // Check that the data is still correct
        esTasks.waitForCorrectDocumentCount(DataPusherContainer.CORRECT_NUM_DOCS);
    }

    @Ignore
    public void shouldNoHaveStaleData() throws UnirestException, IOException {
        LOGGER.info("Scaling down to 2 nodes");
        scaleNumNodesTo(ipAddress, 2);
        esTasks.waitForGreen();
        esTasks.waitForCorrectDocumentCount(DataPusherContainer.CORRECT_NUM_DOCS);
        List<String> esAddresses = esTasks.getEsHttpAddressList();

        JsonNode body = Unirest.delete("http://" + esAddresses.get(0) + "/shakespeare-2016.02").asJson().getBody();
        LOGGER.info("Deleting data " + body);

        LOGGER.info("Scaling back to 3 nodes");
        scaleNumNodesTo(ipAddress, 3);
        esTasks.waitForGreen();

        int documentCount = Unirest.get("http://" + esAddresses.get(0) + "/_count").asJson().getBody().getArray().getJSONObject(0).getInt("count");
        assertEquals("There should not be any stale data but there are " + documentCount + " documents in the cluster", 0, documentCount);
    }

    public void scaleNumNodesTo(String ipAddress, Integer newNumNodes) throws UnirestException {
        HttpResponse<JsonNode> response = Unirest.put("http://" + ipAddress + ":" + WEBUI_PORT + "/v1/cluster/elasticsearchNodes").header("Content-Type", "application/json").body(newNumNodes.toString()).asJson();
        assertEquals(200, response.getStatus());

        Awaitility.await().atMost(60, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).until(() -> {
            String numNodes = getNumberOfNodes(ipAddress);
            LOGGER.debug("Number of nodes: " + numNodes);
            return numNodes.equals(newNumNodes.toString());
        });
        assertEquals(newNumNodes.toString(), getNumberOfNodes(ipAddress));
    }

    public String getNumberOfNodes(String ipAddress) throws UnirestException {
        return Unirest.get("http://" + ipAddress + ":" + WEBUI_PORT + "/v1/cluster/elasticsearchNodes").asJson().getBody().getObject().get("value").toString();
    }
}