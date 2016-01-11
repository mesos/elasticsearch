package org.apache.mesos.elasticsearch.systemtest;

import com.jayway.awaitility.Awaitility;
import com.mashape.unirest.http.Unirest;
import org.apache.log4j.Logger;
import org.apache.mesos.elasticsearch.systemtest.base.SchedulerTestBase;
import org.apache.mesos.elasticsearch.systemtest.containers.DataPusherContainer;
import org.json.JSONArray;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


/**
 * Tests scheduler APIs
 */
public class DataRetrievableAllNodesSystemTest extends SchedulerTestBase {

    private static final Logger LOGGER = Logger.getLogger(DataRetrievableAllNodesSystemTest.class);

    private DataPusherContainer pusher;

    @Test
    public void testDataConsistency() throws Exception {
        ESTasks esTasks = new ESTasks(TEST_CONFIG, getScheduler().getIpAddress(), true);
        waitForGreen(esTasks);
        // TODO (pnw): This is done everywhere. Refactor into utility class.
        List<String> esAddresses = esTasks.getTasks().stream().map(task -> task.getString("http_address")).collect(Collectors.toList());

        pusher = new DataPusherContainer(CLUSTER_ARCHITECTURE.dockerClient, esAddresses.get(0));
        CLUSTER.addAndStartContainer(pusher, TEST_CONFIG.getClusterTimeout());

        LOGGER.info("Addresses:");
        LOGGER.info(esAddresses);
        // Todo (pnw): Refactor me and scaling system test so we DRY
        Awaitility.await().atMost(1, TimeUnit.MINUTES).pollDelay(2, TimeUnit.SECONDS).until(() -> {
            JSONArray responseElements;
            for (String httpAddress : esAddresses) {
                try {
                    responseElements = Unirest.get("http://" + httpAddress + "/_count").asJson().getBody().getArray();
                    LOGGER.info(responseElements);
                    int count = responseElements.getJSONObject(0).getInt("count");
                    if (count == 0 || count % 10 != 0) { // There are ten documents in the data pusher. This is for repeated testing, if used.
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

    // Todo (pnw): Refactor me and scaling system test so we DRY
    public void waitForGreen(ESTasks esTasks) {
        LOGGER.debug("Wating for green.");
        Awaitility.await().atMost(5, TimeUnit.MINUTES).pollInterval(1, TimeUnit.SECONDS).until(() -> { // This can take some time, somtimes.
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
}