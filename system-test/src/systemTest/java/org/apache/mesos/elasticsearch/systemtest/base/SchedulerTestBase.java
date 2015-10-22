package org.apache.mesos.elasticsearch.systemtest.base;

import org.apache.log4j.Logger;
import org.apache.mesos.elasticsearch.systemtest.ElasticsearchSchedulerContainer;
import org.apache.mesos.elasticsearch.systemtest.TasksResponse;
import org.apache.mesos.elasticsearch.systemtest.callbacks.ElasticsearchNodesResponse;
import org.json.JSONObject;
import org.junit.BeforeClass;

import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * A TestBase that starts a scheduler
 */
public abstract class SchedulerTestBase extends TestBase {
    private static final Logger LOGGER = Logger.getLogger(SchedulerTestBase.class);
    private static ElasticsearchSchedulerContainer scheduler;

    @BeforeClass
    public static void startScheduler() throws Exception {
        LOGGER.info("Starting Elasticsearch scheduler");

        scheduler = new ElasticsearchSchedulerContainer(CLUSTER.getConfig().dockerClient, CLUSTER.getZkContainer().getIpAddress());
        CLUSTER.addAndStartContainer(scheduler);

        LOGGER.info("Started Elasticsearch scheduler on " + scheduler.getIpAddress() + ":" + getTestConfig().getSchedulerGuiPort());

        TasksResponse tasksResponse = new TasksResponse(getScheduler().getIpAddress(), CLUSTER.getConfig().getNumberOfSlaves());

        List<JSONObject> tasks = tasksResponse.getTasks();

        ElasticsearchNodesResponse nodesResponse = new ElasticsearchNodesResponse(tasks, CLUSTER.getConfig().getNumberOfSlaves());
        assertTrue("Elasticsearch nodes did not discover each other within 5 minutes", nodesResponse.isDiscoverySuccessful());
    }

    public static ElasticsearchSchedulerContainer getScheduler() {
        return scheduler;
    }
}
