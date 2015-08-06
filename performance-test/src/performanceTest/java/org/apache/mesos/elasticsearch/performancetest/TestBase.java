package org.apache.mesos.elasticsearch.performancetest;

import com.jayway.awaitility.Awaitility;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.apache.mesos.mini.MesosCluster;
import org.apache.mesos.mini.mesos.MesosClusterConfig;
import org.json.JSONObject;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Base test class which launches Mesos CLUSTER and Elasticsearch scheduler
 */
@SuppressWarnings("PMD.AvoidUsingHardCodedIP")
public abstract class TestBase {

    protected static final int NODE_COUNT = 3;

    protected static final MesosClusterConfig CONFIG = MesosClusterConfig.builder()
            .numberOfSlaves(NODE_COUNT)
            .privateRegistryPort(15000) // Currently you have to choose an available port by yourself
            .slaveResources(new String[]{"ports(*):[9200-9200,9300-9300]", "ports(*):[9201-9201,9301-9301]", "ports(*):[9202-9202,9302-9302]"})
            .build();

    private static final Logger LOGGER = Logger.getLogger(TestBase.class);

    @ClassRule
    public static final MesosCluster CLUSTER = new MesosCluster(CONFIG);

    private static ElasticsearchSchedulerContainer scheduler;

    private static DataPusherContainer pusher;

    private static List<String> slavesElasticAddresses = new ArrayList<>();

    /**
     *
     */
    public static class ElasticsearchPusherStarter implements Callable<Boolean> {

        @Override
        public Boolean call() throws Exception {
            try {
                if (!(Unirest.get("http://" + slavesElasticAddresses.get(0) + "/_nodes").asJson().getBody().getObject().getJSONObject("nodes").length() == 3)) {
                    return false;
                }
                pusher = new DataPusherContainer(CONFIG.dockerClient, slavesElasticAddresses.get(0));
                CLUSTER.addAndStartContainer(pusher);
                return true;
            } catch (UnirestException e) {
                return false;
            }
        }
    }

    @BeforeClass
    public static void startScheduler() throws Exception {
        CLUSTER.injectImage("mesos/elasticsearch-executor");

        LOGGER.info("Starting Elasticsearch scheduler");

        scheduler = new ElasticsearchSchedulerContainer(CONFIG.dockerClient, CLUSTER.getMesosContainer().getIpAddress());
        CLUSTER.addAndStartContainer(scheduler);

        LOGGER.info("Started Elasticsearch scheduler on " + scheduler.getIpAddress() + ":8080");
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

    }

    private static class ElasticSearchDataInserted implements Callable<Boolean> {
        public Boolean call() throws Exception {
            InputStream exec = getPusher().getLogStreamStdOut();
            String log = IOUtils.toString(exec);

            if (log.contains("riemann.elastic - elasticized")) {
                return true;
            }

            return false;
        }
    }

    public static ElasticsearchSchedulerContainer getScheduler() {
        return scheduler;
    }

    public static DataPusherContainer getPusher() {
        return pusher;
    }

    public static List<String> getSlavesElasticAddresses() {
        return slavesElasticAddresses;
    }
}
