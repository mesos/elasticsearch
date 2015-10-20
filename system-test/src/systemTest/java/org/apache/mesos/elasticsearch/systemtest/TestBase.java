package org.apache.mesos.elasticsearch.systemtest;

import org.apache.log4j.Logger;
import org.apache.mesos.mini.MesosCluster;
import org.apache.mesos.mini.mesos.MesosClusterConfig;
import org.junit.*;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

/**
 * Base test class which launches Mesos CLUSTER and Elasticsearch scheduler
 */
public abstract class TestBase {

    protected static final Configuration TEST_CONFIG = new Configuration();

    @ClassRule
    public static final MesosCluster CLUSTER = new MesosCluster(
        MesosClusterConfig.builder()
            .numberOfSlaves(TEST_CONFIG.getElasticsearchNodesCount())
            .privateRegistryPort(TEST_CONFIG.getPrivateRegistryPort()) // Currently you have to choose an available port by yourself
            .slaveResources(TEST_CONFIG.getPortRanges())
            .build()
    );

    private static final Logger LOGGER = Logger.getLogger(TestBase.class);
    private static ElasticsearchSchedulerContainer scheduler;

    @Rule
    public TestWatcher watchman = new TestWatcher() {
        @Override
        protected void failed(Throwable e, Description description) {
            CLUSTER.stop();
            scheduler.remove();
        }
    };

    @BeforeClass
    public static void startScheduler() throws Exception {
        CLUSTER.injectImage(TEST_CONFIG.getExecutorImageName());

        LOGGER.info("Starting Elasticsearch scheduler");

        scheduler = new ElasticsearchSchedulerContainer(CLUSTER.getConfig().dockerClient, CLUSTER.getMesosContainer().getIpAddress());
        CLUSTER.addAndStartContainer(scheduler);

        LOGGER.info("Started Elasticsearch scheduler on " + scheduler.getIpAddress() + ":" + TEST_CONFIG.getSchedulerGuiPort());
    }

    public static ElasticsearchSchedulerContainer getScheduler() {
        return scheduler;
    }

}
