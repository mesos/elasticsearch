package org.apache.mesos.elasticsearch.systemtest;

import com.jayway.awaitility.Awaitility;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.log4j.Logger;
import org.apache.mesos.elasticsearch.systemtest.base.SchedulerTestBase;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tests the scaling capabilities. To run multiple times, uncomment the code below.
 */
//@RunWith(Parameterized.class)
@Ignore("This test has to be merged into DeploymentSystemTest. See https://github.com/mesos/elasticsearch/issues/591")
public class ScalingSystemTest extends SchedulerTestBase {
    private static final Logger LOGGER = Logger.getLogger(ScalingSystemTest.class);
    public static final int WEBUI_PORT = 31100;
    public static final int NUM_TEST_DOCS = 10;
    private String ipAddress;
    private ESTasks esTasks;

//    @Parameterized.Parameters
//    public static List<Object[]> data() {
//        return Arrays.asList(new Object[10][0]);
//    }

    @Before
    public void before() {
        ipAddress = getScheduler().getIpAddress();
        esTasks = new ESTasks(TEST_CONFIG, ipAddress);
    }

    @Test
    public void shouldScaleDown() {
        scaleNumNodesTo(ipAddress, 3); // Reset to 3 nodes
        esTasks.waitForGreen(3);
        LOGGER.debug("Number of nodes: " + getNumberOfNodes(ipAddress));
        scaleNumNodesTo(ipAddress, 2);
        esTasks.waitForGreen(2);
    }

    @Test
    public void shouldScaleUp() {
        scaleNumNodesTo(ipAddress, 2); // Reset to 2 nodes
        esTasks.waitForGreen(2);
        LOGGER.debug("Number of nodes: " + getNumberOfNodes(ipAddress));
        scaleNumNodesTo(ipAddress, 3);
        esTasks.waitForGreen(3);
    }

    @Test
    public void shouldNotLoseDataWhenScalingDown() throws UnirestException {
        // Make sure we have three nodes
        scaleNumNodesTo(ipAddress, 3);
        esTasks.waitForGreen(3);

        esTasks.waitForCorrectDocumentCount(0); // Make sure we can actually connect.

        List<String> esAddresses = esTasks.getEsHttpAddressList();
        LOGGER.info("Addresses: " + esAddresses);

        seedData("http://" + esAddresses.get(0));
        LOGGER.info("Started data push");

        esTasks.waitForCorrectDocumentCount(NUM_TEST_DOCS);

        // Scale down to one node
        scaleNumNodesTo(ipAddress, 1);
        esTasks.waitForGreen(1);

        // Check that the data is still correct
        esTasks.waitForCorrectDocumentCount(NUM_TEST_DOCS);
    }

    @Test
    public void shouldNotHaveStaleData() throws UnirestException {
        // Make sure we have three nodes
        scaleNumNodesTo(ipAddress, 3);
        esTasks.waitForGreen(3);
        List<String> esAddresses = esTasks.getEsHttpAddressList();

        Unirest.delete("http://" + esAddresses.get(0) + "/*").asJson();

        esTasks.waitForCorrectDocumentCount(0); // Make sure we can actually connect.

        LOGGER.info("Addresses: " + esAddresses);

        seedData("http://" + esAddresses.get(0));
        LOGGER.info("Started data push");

        LOGGER.info("Scaling down to 2 nodes");
        scaleNumNodesTo(ipAddress, 2);
        esTasks.waitForGreen(2);
        esTasks.waitForCorrectDocumentCount(NUM_TEST_DOCS);
        esAddresses = esTasks.getEsHttpAddressList();

        // Do the delete on two nodes
        JsonNode body = Unirest.delete("http://" + esAddresses.get(0) + "/shakespeare-*").asJson().getBody();
        LOGGER.info("Deleting data " + body);

        // Scale up to three nodes, there should still be zero docs.
        LOGGER.info("Scaling back to 3 nodes");
        scaleNumNodesTo(ipAddress, 3);
        esTasks.waitForGreen(3);

        esTasks.waitForCorrectDocumentCount(0); // Ensure there are zero docs.
    }

    
    private void seedData(String esAddresses) {
        try {
            for (int i = 0; i < NUM_TEST_DOCS; i++) {
                Awaitility.await().atMost(10L, TimeUnit.SECONDS).until(() -> Unirest.post(esAddresses + "/dummy/data")
                        .body("{ \"user\" : \"kimchy\", \"post_date\" : \"2009-11-15T14:12:12\", \"message\" : \"trying out Elasticsearch\" }")
                        .asString().getStatus() == 201);
            }
        } catch (Exception e) {
            LOGGER.debug(e);
        }
    }

    public void scaleNumNodesTo(String ipAddress, Integer newNumNodes) {
        Awaitility.await().atMost(60, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).until(() -> {
            Unirest.put("http://" + ipAddress + ":" + WEBUI_PORT + "/v1/cluster/elasticsearchNodes").header("Content-Type", "application/json").body(newNumNodes.toString()).asJson();
            String numNodes = getNumberOfNodes(ipAddress);
            LOGGER.debug("Number of nodes: " + numNodes);
            return numNodes.equals(newNumNodes.toString());
        });
    }

    public String getNumberOfNodes(String ipAddress) {
        final AtomicReference<String> numNodes = new AtomicReference<>();
        Awaitility.await().pollInterval(1L, TimeUnit.SECONDS).atMost(30L, TimeUnit.SECONDS).until(() -> {
            try {
                numNodes.set(Unirest.get("http://" + ipAddress + ":" + WEBUI_PORT + "/v1/cluster/elasticsearchNodes").asJson().getBody().getObject().get("value").toString());
                return true;
            } catch (Exception e) {
                return false;
            }
        });
        return numNodes.get();
    }
}