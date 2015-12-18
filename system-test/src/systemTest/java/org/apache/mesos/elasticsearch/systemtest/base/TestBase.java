package org.apache.mesos.elasticsearch.systemtest.base;

import com.containersol.minimesos.MesosCluster;
import com.containersol.minimesos.mesos.ClusterArchitecture;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang.StringUtils;
import org.apache.mesos.elasticsearch.systemtest.Configuration;
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
            .withMaster()
            .withSlave()
            .withSlave()
            .withSlave(StringUtils.join(TEST_CONFIG.getPortRanges(), ","))
            .withZooKeeper()
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
