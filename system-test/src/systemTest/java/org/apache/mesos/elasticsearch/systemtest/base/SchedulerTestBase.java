package org.apache.mesos.elasticsearch.systemtest.base;

import com.containersol.minimesos.docker.DockerClientFactory;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.core.DockerClientBuilder;
import org.apache.log4j.Logger;
import org.apache.mesos.elasticsearch.systemtest.ESTasks;
import org.apache.mesos.elasticsearch.systemtest.TasksResponse;
import org.apache.mesos.elasticsearch.systemtest.callbacks.ElasticsearchNodesResponse;
import org.apache.mesos.elasticsearch.systemtest.containers.ElasticsearchSchedulerContainer;
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

        DockerClient dockerClient = DockerClientFactory.build();

        scheduler = new ElasticsearchSchedulerContainer(dockerClient, CLUSTER.getZooKeeper().getIpAddress());
        CLUSTER.addAndStartProcess(scheduler, TEST_CONFIG.getClusterTimeout());

        LOGGER.info("Started Elasticsearch scheduler on " + scheduler.getIpAddress() + ":" + getTestConfig().getSchedulerGuiPort());

        ESTasks esTasks = new ESTasks(TEST_CONFIG, scheduler.getIpAddress());
        new TasksResponse(esTasks, TEST_CONFIG.getElasticsearchNodesCount());

        ElasticsearchNodesResponse nodesResponse = new ElasticsearchNodesResponse(esTasks, TEST_CONFIG.getElasticsearchNodesCount());
        assertTrue("Elasticsearch nodes did not discover each other within 5 minutes", nodesResponse.isDiscoverySuccessful());
    }

    public static ElasticsearchSchedulerContainer getScheduler() {
        return scheduler;
    }
}
