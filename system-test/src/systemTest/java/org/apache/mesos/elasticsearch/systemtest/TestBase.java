package org.apache.mesos.elasticsearch.systemtest;

import org.apache.log4j.Logger;
import org.apache.mesos.mini.MesosCluster;
import org.apache.mesos.mini.mesos.MesosClusterConfig;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

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
        CLUSTER.injectImage("mesos/elasticsearch-executor");

        LOGGER.info("Starting Elasticsearch scheduler");

        scheduler = new ElasticsearchSchedulerContainer(CONFIG.dockerClient, CLUSTER.getMesosContainer().getIpAddress());
        CLUSTER.addAndStartContainer(scheduler);

        LOGGER.info("Started Elasticsearch scheduler on " + scheduler.getIpAddress() + ":31100");
    }

    public static ElasticsearchSchedulerContainer getScheduler() {
        return scheduler;
    }

}
