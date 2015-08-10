package org.apache.mesos.elasticsearch.systemtest;

import com.jayway.awaitility.Awaitility;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Rule;
import org.junit.Test;
import com.mashape.unirest.http.Unirest;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;


/**
 * Tests scheduler APIs
 */
public class DataRetrievableAllNodesPerformanceTest extends TestBase {

    private static final Logger LOGGER = Logger.getLogger(DataRetrievableAllNodesPerformanceTest.class);

    private static DataPusherContainer pusher;

    private static List<String> slavesElasticAddresses = new ArrayList<>();


    @Rule
    public TestWatcher pusherWatch = new TestWatcher() {
        @Override
        protected void failed(Throwable e, Description description) {
            pusher.remove();
        }
    };


    @Test
    public void testPusherStarted() throws Exception {

        try {
            List<JSONObject> tasks = new TasksResponse(getScheduler().getIpAddress(), NODE_COUNT).getTasks();
            for (JSONObject task : tasks) {
                LOGGER.info(task);
                slavesElasticAddresses.add(task.getString("http_address"));

            }
        } catch (Exception e) {
            LOGGER.error("Exception thrown: " + e.getMessage());
            throw new RuntimeException(e.getMessage());
        }

        Awaitility.await().atMost(2, TimeUnit.MINUTES).pollDelay(2, TimeUnit.SECONDS).until(new ElasticsearchPusherStarter());
        Awaitility.await().atMost(2, TimeUnit.MINUTES).pollDelay(2, TimeUnit.SECONDS).until(new ElasticSearchDataInserted());


        LOGGER.info("Addresses:");
        LOGGER.info(getSlavesElasticAddresses());
        Awaitility.await().atMost(20, TimeUnit.SECONDS).pollDelay(2, TimeUnit.SECONDS).until(new FetchAllNodesData());
    }

    /**
     *
     */
    public static class ElasticsearchPusherStarter implements Callable<Boolean> {

        @Override
        public Boolean call() throws Exception {
            try {
                if (!(Unirest.get("http://" + getSlavesElasticAddresses().get(0) + "/_nodes").asJson().getBody().getObject().getJSONObject("nodes").length() == 3)) {
                    return false;
                }
                pusher = new DataPusherContainer(CONFIG.dockerClient, getSlavesElasticAddresses().get(0));
                CLUSTER.addAndStartContainer(pusher);
                return true;
            } catch (UnirestException e) {
                return false;
            }
        }
    }

    /**
     *
     */
    public static class ElasticSearchDataInserted implements Callable<Boolean> {
        public Boolean call() throws Exception {
            InputStream exec = getPusher().getLogStreamStdOut();
            String log = IOUtils.toString(exec);

            if (log.contains("riemann.elastic - elasticized")) {
                return true;
            }

            return false;
        }
    }


    private static class FetchAllNodesData implements Callable<Boolean> {

        @Override
        public Boolean call() throws Exception {
            JSONArray responseElements;
            for (String httpAddress: DataRetrievableAllNodesPerformanceTest.getSlavesElasticAddresses()) {
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
        }
    }

    public static DataPusherContainer getPusher() {
        return pusher;
    }

    public static List<String> getSlavesElasticAddresses() {
        return slavesElasticAddresses;
    }

}