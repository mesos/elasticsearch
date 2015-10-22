package org.apache.mesos.elasticsearch.systemtest.base;

import org.apache.log4j.Logger;
import org.apache.mesos.elasticsearch.systemtest.ElasticsearchSchedulerContainer;
import org.junit.BeforeClass;

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
    }

    public static ElasticsearchSchedulerContainer getScheduler() {
        return scheduler;
    }
}
