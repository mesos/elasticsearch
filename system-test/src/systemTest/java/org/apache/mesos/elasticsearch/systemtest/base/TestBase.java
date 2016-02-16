package org.apache.mesos.elasticsearch.systemtest.base;

import com.containersol.minimesos.cluster.MesosCluster;
import com.containersol.minimesos.mesos.ClusterArchitecture;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.mesos.elasticsearch.systemtest.Configuration;
import org.apache.mesos.elasticsearch.systemtest.containers.AlpineContainer;
import org.apache.mesos.elasticsearch.systemtest.util.DockerUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

/**
 * Base test class which launches Mesos CLUSTER
 */
@SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD", "MS_PKGPROTECT", "MS_CANNOT_BE_FINAL"})
public abstract class TestBase {

    protected static final Configuration TEST_CONFIG = new Configuration();

    protected static ClusterArchitecture CLUSTER_ARCHITECTURE;

    protected static MesosCluster CLUSTER;

    @ClassRule
    public static final TestWatcher WATCHER = new TestWatcher() {
        @Override
        protected void failed(Throwable e, Description description) {
            CLUSTER.stop();
        }
    };

    @BeforeClass
    public static void reinitialiseCluster() {
        CLUSTER_ARCHITECTURE = new ClusterArchitecture.Builder()
                .withZooKeeper()
                .withMaster()
                .withSlave(TEST_CONFIG.getPortRanges()[0])
                .withSlave(TEST_CONFIG.getPortRanges()[1])
                .withSlave(TEST_CONFIG.getPortRanges()[2])
                .build();
        CLUSTER = new MesosCluster(CLUSTER_ARCHITECTURE);
        CLUSTER.start(60);
    }

    @BeforeClass
    public static void prepareCleanDockerEnvironment() {
        new DockerUtil(CLUSTER_ARCHITECTURE.dockerClient).killAllSchedulers();
        new DockerUtil(CLUSTER_ARCHITECTURE.dockerClient).killAllExecutors();

        // Completely wipe out old data dir, just in case there is any old data in there.
        String dataDir = org.apache.mesos.elasticsearch.scheduler.Configuration.DEFAULT_HOST_DATA_DIR;
        AlpineContainer alpineContainer = new AlpineContainer(CLUSTER_ARCHITECTURE.dockerClient, dataDir, dataDir, new String[]{"sh", "-c", "rm -rf " + dataDir + "/* ; sleep 9999 ;"});
        alpineContainer.start(TEST_CONFIG.getClusterTimeout());
        alpineContainer.remove();
    }

    @AfterClass
    public static void killAllContainers() {
        CLUSTER.stop();
        new DockerUtil(CLUSTER_ARCHITECTURE.dockerClient).killAllExecutors();
    }

    public static Configuration getTestConfig() {
        return TEST_CONFIG;
    }
}
