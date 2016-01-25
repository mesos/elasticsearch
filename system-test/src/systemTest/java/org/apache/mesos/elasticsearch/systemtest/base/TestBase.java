package org.apache.mesos.elasticsearch.systemtest.base;

import com.containersol.minimesos.MesosCluster;
import com.containersol.minimesos.mesos.*;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.mesos.elasticsearch.systemtest.Configuration;
import org.apache.mesos.elasticsearch.systemtest.containers.AlpineContainer;
import org.apache.mesos.elasticsearch.systemtest.util.DockerUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.util.Collections;

/**
 * Base test class which launches Mesos CLUSTER
 */
@SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD", "MS_PKGPROTECT", "MS_CANNOT_BE_FINAL"})
public abstract class TestBase {

    protected static final Configuration TEST_CONFIG = new Configuration();
    public static final String MESOS_VERSION_TAG = "0.26.0-0.2.145.ubuntu1404";

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
                .withMaster(VersionedMaster::new)
                .withSlave(zookeeper -> new VersionedSlave(zookeeper, TEST_CONFIG.getPortRanges()[0]))
                .withSlave(zookeeper -> new VersionedSlave(zookeeper, TEST_CONFIG.getPortRanges()[1]))
                .withSlave(zookeeper -> new VersionedSlave(zookeeper, TEST_CONFIG.getPortRanges()[2]))
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

    private static class VersionedMaster extends MesosMasterExtended {
        public VersionedMaster(ZooKeeper zooKeeperContainer) {
            super(DockerClientFactory.build(), zooKeeperContainer, MesosMasterExtended.MESOS_MASTER_IMAGE, MESOS_VERSION_TAG, Collections.emptyMap(), true);
        }
    }

    private static class VersionedSlave extends MesosSlaveExtended {
        public VersionedSlave(ZooKeeper zooKeeperContainer, String resources) {
            super(DockerClientFactory.build(), resources, String.valueOf(MesosSlaveExtended.MESOS_SLAVE_PORT), zooKeeperContainer, MesosSlaveExtended.MESOS_SLAVE_IMAGE, MESOS_VERSION_TAG);
        }
    }
}
