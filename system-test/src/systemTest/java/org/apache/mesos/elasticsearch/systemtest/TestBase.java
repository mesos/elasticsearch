package org.apache.mesos.elasticsearch.systemtest;

import org.apache.log4j.Logger;
import org.apache.mesos.mini.MesosCluster;
import org.apache.mesos.mini.mesos.MesosClusterConfig;
import org.junit.BeforeClass;
import org.junit.ClassRule;

/**
 * Base test class which launches Mesos cluster and Elasticsearch scheduler
 */
@SuppressWarnings("PMD.AvoidUsingHardCodedIP")
public abstract class TestBase {

    protected static final MesosClusterConfig config = MesosClusterConfig.builder()
            .numberOfSlaves(3)
            .privateRegistryPort(15000) // Currently you have to choose an available port by yourself
            .slaveResources(new String[]{"ports(*):[9200-9200,9300-9300]", "ports(*):[9201-9201,9301-9301]", "ports(*):[9202-9202,9302-9302]"})
            .build();

    private static final Logger LOGGER = Logger.getLogger(TestBase.class);

    @ClassRule
    public static final MesosCluster cluster = new MesosCluster(config);

    private static ElasticsearchSchedulerContainer scheduler;

    @BeforeClass
    public static void startScheduler() throws Exception {
        cluster.injectImage("mesos/elasticsearch-executor");

        LOGGER.info("Starting Elasticsearch scheduler");

        scheduler = new ElasticsearchSchedulerContainer(config.dockerClient, cluster.getMesosContainer().getIpAddress());
        cluster.addAndStartContainer(scheduler);

        LOGGER.info("Started Elasticsearch scheduler on " + scheduler.getIpAddress() + ":8080");
    }

    public static ElasticsearchSchedulerContainer getScheduler() {
        return scheduler;
    }
}
