package org.apache.mesos.elasticsearch.systemtest.base;

import com.containersol.minimesos.MesosCluster;
import com.containersol.minimesos.mesos.MesosClusterConfig;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.mesos.elasticsearch.systemtest.Configuration;
import org.apache.mesos.elasticsearch.systemtest.Main;
import org.apache.mesos.elasticsearch.systemtest.util.DockerUtil;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

/**
 * Base test class which launches Mesos CLUSTER
 */
@SuppressFBWarnings("URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
public abstract class TestBase {
    private static final Configuration TEST_CONFIG = new Configuration();

    @ClassRule
    public static final MesosCluster CLUSTER = new MesosCluster(
        MesosClusterConfig.builder()
                .mesosImageTag(Main.MESOS_IMAGE_TAG)
                .slaveResources(TEST_CONFIG.getPortRanges())
                .build()
    );

    @ClassRule
    public static final TestWatcher WATCHER = new TestWatcher() {
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

    public static Configuration getTestConfig() {
        return TEST_CONFIG;
    }
}
