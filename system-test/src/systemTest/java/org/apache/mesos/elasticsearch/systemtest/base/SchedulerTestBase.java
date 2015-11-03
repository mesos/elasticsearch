package org.apache.mesos.elasticsearch.systemtest.base;

import org.apache.log4j.Logger;
import org.apache.mesos.elasticsearch.systemtest.ESTasks;
import org.apache.mesos.elasticsearch.systemtest.ElasticsearchSchedulerContainer;
import org.apache.mesos.elasticsearch.systemtest.TasksResponse;
import org.apache.mesos.elasticsearch.systemtest.callbacks.ElasticsearchNodesResponse;
import org.junit.BeforeClass;

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

        scheduler = new ElasticsearchSchedulerContainer(CLUSTER.getConfig().dockerClient, CLUSTER.getZkContainer().getIpAddress(), CLUSTER);
        CLUSTER.addAndStartContainer(scheduler);

        LOGGER.info("Started Elasticsearch scheduler on " + scheduler.getIpAddress() + ":" + getTestConfig().getSchedulerGuiPort());

        ESTasks esTasks = new ESTasks(TEST_CONFIG, scheduler.getIpAddress());
        new TasksResponse(esTasks, CLUSTER.getConfig().getNumberOfSlaves());

        ElasticsearchNodesResponse nodesResponse = new ElasticsearchNodesResponse(esTasks, CLUSTER.getConfig().getNumberOfSlaves());
        assertTrue("Elasticsearch nodes did not discover each other within 5 minutes", nodesResponse.isDiscoverySuccessful());
    }

    public static ElasticsearchSchedulerContainer getScheduler() {
        return scheduler;
    }
}
