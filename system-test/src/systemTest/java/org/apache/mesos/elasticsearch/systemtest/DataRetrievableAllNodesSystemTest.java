package org.apache.mesos.elasticsearch.systemtest;

import com.jayway.awaitility.Awaitility;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.log4j.Logger;
import org.apache.mesos.elasticsearch.systemtest.base.SchedulerTestBase;
import org.apache.mesos.elasticsearch.systemtest.containers.DataPusherContainer;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;


/**
 * Tests scheduler APIs
 */
public class DataRetrievableAllNodesSystemTest extends SchedulerTestBase {

    private static final Logger LOGGER = Logger.getLogger(DataRetrievableAllNodesSystemTest.class);

    private static DataPusherContainer pusher;

    private static List<String> slavesElasticAddresses = new ArrayList<>();

    @BeforeClass
    public static void startDataPusher() {

        try {
            List<JSONObject> tasks = new TasksResponse(getScheduler().getIpAddress(), CLUSTER.getConfig().getNumberOfSlaves()).getTasks();
            for (JSONObject task : tasks) {
                LOGGER.info(task);
                slavesElasticAddresses.add(task.getString("http_address"));

            }
        } catch (Exception e) {
            LOGGER.error("Exception thrown: " + e.getMessage());
            throw new RuntimeException(e.getMessage());
        }

        Awaitility.await().atMost(5, TimeUnit.MINUTES).pollDelay(2, TimeUnit.SECONDS).until(() -> {
            try {
                if (!(Unirest.get("http://" + getSlavesElasticAddresses().get(0) + "/_nodes").asJson().getBody().getObject().getJSONObject("nodes").length() == 3)) {
                    return false;
                }
                pusher = new DataPusherContainer(CLUSTER.getConfig().dockerClient, getSlavesElasticAddresses().get(0));
                CLUSTER.addAndStartContainer(pusher);
                return true;
            } catch (UnirestException e) {
                return false;
            }
        });
    }

    @Test
    public void testDataConsistency() throws Exception {
        LOGGER.info("Addresses:");
        LOGGER.info(getSlavesElasticAddresses());
        Awaitility.await().atMost(1, TimeUnit.MINUTES).pollDelay(2, TimeUnit.SECONDS).until(() -> {
            JSONArray responseElements;
            for (String httpAddress: DataRetrievableAllNodesSystemTest.getSlavesElasticAddresses()) {
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

    public static DataPusherContainer getPusher() {
        return pusher;
    }

    public static List<String> getSlavesElasticAddresses() {
        return slavesElasticAddresses;
    }

}