package org.apache.mesos.elasticsearch.systemtest.base;

import com.containersol.minimesos.MesosCluster;
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
@SuppressFBWarnings("URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
public abstract class TestBase {

    protected static final Configuration TEST_CONFIG = new Configuration();

    protected static final ClusterArchitecture clusterArchitecture = new ClusterArchitecture.Builder()
            .withZooKeeper()
            .withMaster()
            .withSlave(TEST_CONFIG.getPortRanges()[0])
            .withSlave(TEST_CONFIG.getPortRanges()[1])
            .withSlave(TEST_CONFIG.getPortRanges()[2])
            .build();

    @ClassRule
    public static final MesosCluster CLUSTER = new MesosCluster(clusterArchitecture);

    @ClassRule
    public static final TestWatcher WATCHER = new TestWatcher() {
        @Override
        protected void failed(Throwable e, Description description) {
            CLUSTER.stop();
        }
    };

    @BeforeClass
    public static void prepareCleanDockerEnvironment() {
        new DockerUtil(clusterArchitecture.dockerClient).killAllSchedulers();
        new DockerUtil(clusterArchitecture.dockerClient).killAllExecutors();

        // Completely wipe out old data dir, just in case there is any old data in there.
        String dataDir = org.apache.mesos.elasticsearch.scheduler.Configuration.DEFAULT_HOST_DATA_DIR;
        AlpineContainer alpineContainer = new AlpineContainer(clusterArchitecture.dockerClient, dataDir, dataDir, new String[]{"sh", "-c", "rm -rf " + dataDir + "/* ; sleep 9999 ;"});
        alpineContainer.start(TEST_CONFIG.getClusterTimeout());
        alpineContainer.remove();
    }

    @AfterClass
    public static void killAllContainers() {
        CLUSTER.stop();
        new DockerUtil(clusterArchitecture.dockerClient).killAllExecutors();
    }

    public static Configuration getTestConfig() {
        return TEST_CONFIG;
    }
}
