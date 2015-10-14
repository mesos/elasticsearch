package org.apache.mesos.elasticsearch.systemtest;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.mesos.mini.MesosCluster;
import org.apache.mesos.mini.mesos.MesosClusterConfig;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;

/**
 * Base test class which launches Mesos CLUSTER and Elasticsearch scheduler
 */
@SuppressWarnings("PMD.AvoidUsingHardCodedIP")
public abstract class TestBase {

    @ClassRule
    public static final MesosCluster CLUSTER = new MesosCluster(
        MesosClusterConfig.builder()
            .numberOfSlaves(3)
            .privateRegistryPort(15000) // Currently you have to choose an available port by yourself
            .slaveResources(new String[]{"ports(*):[9200-9200,9300-9300]", "ports(*):[9201-9201,9301-9301]", "ports(*):[9202-9202,9302-9302]"})
            .build()
    );

    private static final Logger LOGGER = Logger.getLogger(TestBase.class);

    private static ElasticsearchSchedulerContainer scheduler;

    @Rule
    public TestWatcher watchman = new TestWatcher() {
        @Override
        protected void failed(Throwable e, Description description) {
            LOGGER.error("Scheduler logs: " + getLogs(scheduler.getContainerId()) + "\n ##########");
            CLUSTER.getContainers().forEach(container -> {
                LOGGER.warn(container.getContainerId() + " logs: " + getLogs(container.getContainerId()) + "\n ##########");
            });
            CLUSTER.stop();
            scheduler.remove();
        }
    };

    protected String getLogs(String containerId) {
        InputStream stream = CLUSTER.getConfig().dockerClient.logContainerCmd(containerId).withStdOut().withStdErr().exec();
        try {
            StringWriter output = new StringWriter();
            IOUtils.copy(stream, output);
            return StringUtils.replace(output.toString(), System.lineSeparator(), System.lineSeparator() + "[" + containerId + "]: ");
        } catch (IOException e) {
            e.printStackTrace();
            return "";
        }
    }

    @BeforeClass
    public static void startScheduler() throws Exception {
        CLUSTER.injectImage("mesos/elasticsearch-executor");

        LOGGER.info("Starting Elasticsearch scheduler");

        scheduler = new ElasticsearchSchedulerContainer(CLUSTER.getConfig().dockerClient, CLUSTER.getMesosContainer().getIpAddress());
        CLUSTER.addAndStartContainer(scheduler);

        LOGGER.info("Started Elasticsearch scheduler on " + scheduler.getIpAddress() + ":31100");
    }

    public static ElasticsearchSchedulerContainer getScheduler() {
        return scheduler;
    }

}
