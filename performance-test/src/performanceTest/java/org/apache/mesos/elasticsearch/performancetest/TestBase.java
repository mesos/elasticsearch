package org.apache.mesos.elasticsearch.performancetest;

import com.jayway.awaitility.Awaitility;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.log4j.Logger;
import org.apache.mesos.mini.MesosCluster;
import org.apache.mesos.mini.mesos.MesosClusterConfig;
import org.json.JSONObject;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Base test class which launches Mesos CLUSTER and Elasticsearch scheduler
 */
@SuppressWarnings("PMD.AvoidUsingHardCodedIP")
public abstract class TestBase {

    protected static final MesosClusterConfig CONFIG = MesosClusterConfig.builder()
            .numberOfSlaves(3)
            .privateRegistryPort(15000) // Currently you have to choose an available port by yourself
            .slaveResources(new String[]{"ports(*):[9200-9200,9300-9300]", "ports(*):[9201-9201,9301-9301]", "ports(*):[9202-9202,9302-9302]"})
            .build();

    private static final Logger LOGGER = Logger.getLogger(TestBase.class);

    @ClassRule
    public static final MesosCluster CLUSTER = new MesosCluster(CONFIG);

    private static ElasticsearchSchedulerContainer scheduler;

    private static DataPusherContainer pusher;

    private static String slaveHttpAddress;

    /**
     *
     */
    public static class ElasticsearchPusherStarter implements Callable<Boolean> {

        @Override
        public Boolean call() throws Exception {
            try {
                if (!(Unirest.get("http://" + slaveHttpAddress + "/_nodes").asJson().getBody().getObject().getJSONObject("nodes").length() == 3)) {
                    return false;
                }
                pusher = new DataPusherContainer(CONFIG.dockerClient, slaveHttpAddress);
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
        slaveHttpAddress = "";
        try {
            TasksResponse tasksResponse = new TasksResponse(getScheduler().getIpAddress());
            JSONObject taskObject = tasksResponse.getJson().getBody().getArray().getJSONObject(0);
            slaveHttpAddress = taskObject.getString("http_address");
        } catch (Exception e) {
            System.out.println(e.getMessage());
            throw new RuntimeException(e.getMessage());
        }

        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(new ElasticsearchPusherStarter());

    }

    public static ElasticsearchSchedulerContainer getScheduler() {
        return scheduler;
    }

    public static DataPusherContainer getPusher() {
        return pusher;
    }
}
