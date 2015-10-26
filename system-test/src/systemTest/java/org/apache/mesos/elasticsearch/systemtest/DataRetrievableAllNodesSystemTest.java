package org.apache.mesos.elasticsearch.systemtest;

import com.jayway.awaitility.Awaitility;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.log4j.Logger;
import org.apache.mesos.elasticsearch.systemtest.base.SchedulerTestBase;
import org.apache.mesos.elasticsearch.systemtest.callbacks.ElasticsearchNodesResponse;
import org.apache.mesos.elasticsearch.systemtest.containers.DataPusherContainer;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.Assert.assertTrue;


/**
 * Tests scheduler APIs
 */
public class DataRetrievableAllNodesSystemTest extends SchedulerTestBase {

    private static final Logger LOGGER = Logger.getLogger(DataRetrievableAllNodesSystemTest.class);

    private DataPusherContainer pusher;
    private List<String> esAddresses;

    @Test
    public void testDataConsistency() throws Exception {
        ESTasks esTasks = new ESTasks(TEST_CONFIG, getScheduler().getIpAddress());
        Awaitility.await().atMost(5, TimeUnit.MINUTES).pollDelay(2, TimeUnit.SECONDS).until(() -> {
            try {
                esAddresses = esTasks.getTasks().stream().map(task -> task.getString("http_address")).collect(Collectors.toList());
                // This may throw a JSONException if we call before the JSON has been generated. Hence, catch exception.
                return Unirest.get("http://" + esAddresses.get(0) + "/_nodes").asJson().getBody().getObject().getJSONObject("nodes").length() == 3;
            } catch (Exception e) {
                return false;
            }
        });

        pusher = new DataPusherContainer(CLUSTER.getConfig().dockerClient, esAddresses.get(0));
        CLUSTER.addAndStartContainer(pusher);

        LOGGER.info("Addresses:");
        LOGGER.info(esAddresses);
        Awaitility.await().atMost(1, TimeUnit.MINUTES).pollDelay(2, TimeUnit.SECONDS).until(() -> {
            JSONArray responseElements;
            for (String httpAddress: esAddresses) {
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
    }
}