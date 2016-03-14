package org.apache.mesos.elasticsearch.systemtest.base;

import com.containersol.minimesos.cluster.MesosCluster;
import com.containersol.minimesos.mesos.ClusterArchitecture;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.mesos.elasticsearch.systemtest.Configuration;
import org.apache.mesos.elasticsearch.systemtest.containers.AlpineContainer;
import org.apache.mesos.elasticsearch.systemtest.containers.MesosMasterTagged;
import org.apache.mesos.elasticsearch.systemtest.containers.MesosSlaveTagged;
import org.apache.mesos.elasticsearch.systemtest.util.DockerUtil;
import org.apache.mesos.elasticsearch.systemtest.util.IpTables;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base test class which launches Mesos CLUSTER
 */
@SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD", "MS_PKGPROTECT", "MS_CANNOT_BE_FINAL"})
public abstract class TestBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestBase.class);
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
                .withMaster(MesosMasterTagged::new)
                .withAgent(zooKeeper -> new MesosSlaveTagged(zooKeeper, TEST_CONFIG.getPortRanges().get(0)))
                .withAgent(zooKeeper -> new MesosSlaveTagged(zooKeeper, TEST_CONFIG.getPortRanges().get(1)))
                .withAgent(zooKeeper -> new MesosSlaveTagged(zooKeeper, TEST_CONFIG.getPortRanges().get(2)))
                .build();
        CLUSTER = new MesosCluster(CLUSTER_ARCHITECTURE);
        CLUSTER.setExposedHostPorts(true);
        CLUSTER.start(TEST_CONFIG.getClusterTimeout());
        IpTables.apply(CLUSTER_ARCHITECTURE.dockerClient, CLUSTER, TEST_CONFIG);
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
