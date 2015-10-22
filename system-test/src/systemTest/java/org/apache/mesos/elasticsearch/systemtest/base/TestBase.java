package org.apache.mesos.elasticsearch.systemtest.base;

import com.containersol.minimesos.MesosCluster;
import com.containersol.minimesos.mesos.MesosClusterConfig;
import org.apache.mesos.elasticsearch.systemtest.Configuration;
import org.apache.mesos.elasticsearch.systemtest.Main;
import org.apache.mesos.elasticsearch.systemtest.util.DockerUtil;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

/**
 * Base test class which launches Mesos CLUSTER
 */
public abstract class TestBase {

    protected static final Configuration TEST_CONFIG = new Configuration();

    @ClassRule
    public static final MesosCluster CLUSTER = new MesosCluster(
        MesosClusterConfig.builder()
                .mesosImageTag(Main.MESOS_IMAGE_TAG)
                .slaveResources(new String[]{"ports(*):[9200-9200,9300-9300]", "ports(*):[9201-9201,9301-9301]", "ports(*):[9202-9202,9302-9302]"})
                .build()
    );

    @Rule
    public TestWatcher watchman = new TestWatcher() {
        @Override
        protected void failed(Throwable e, Description description) {
            CLUSTER.stop();
        }
    };

    @AfterClass
    public static void killAllContainers() {
        CLUSTER.stop();
        new DockerUtil(CLUSTER.getConfig().dockerClient).killAllExecutors();
    }
}
