package org.apache.mesos.elasticsearch.systemtest;

import org.apache.log4j.Logger;
import com.containersol.minimesos.MesosCluster;
import com.containersol.minimesos.mesos.MesosClusterConfig;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
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
                .mesosImageTag("0.22.1-1.0.ubuntu1404")
                .slaveResources(new String[]{"ports(*):[9200-9200,9300-9300]", "ports(*):[9201-9201,9301-9301]", "ports(*):[9202-9202,9302-9302]"})
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
        LOGGER.info("Starting Elasticsearch scheduler");

        scheduler = new ElasticsearchSchedulerContainer(CLUSTER.getConfig().dockerClient, CLUSTER.getZkContainer().getIpAddress());
        CLUSTER.addAndStartContainer(scheduler);

        LOGGER.info("Started Elasticsearch scheduler on " + scheduler.getIpAddress() + ":" + TEST_CONFIG.getSchedulerGuiPort());
    }

    public static ElasticsearchSchedulerContainer getScheduler() {
        return scheduler;
    }

}
