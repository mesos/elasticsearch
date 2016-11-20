package org.apache.mesos.elasticsearch.systemtest.base;

import com.containersol.minimesos.cluster.MesosCluster;
import com.containersol.minimesos.docker.DockerClientFactory;
import com.containersol.minimesos.junit.MesosClusterTestRule;
import com.containersol.minimesos.mesos.MesosClusterContainersFactory;
import com.github.dockerjava.api.DockerClient;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.mesos.elasticsearch.systemtest.Configuration;
import org.apache.mesos.elasticsearch.systemtest.containers.AlpineContainer;
import org.apache.mesos.elasticsearch.systemtest.util.DockerUtil;
import org.apache.mesos.elasticsearch.systemtest.util.IpTables;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;

/**
 * Base test class which launches Mesos CLUSTER
 */
@SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD", "MS_PKGPROTECT", "MS_CANNOT_BE_FINAL"})
public abstract class TestBase {

    protected static final Configuration TEST_CONFIG = new Configuration();

    @ClassRule
    public static final MesosClusterTestRule RULE = MesosClusterTestRule.fromFile("src/systemTest/resources/testMinimesosFile");

    public static final MesosCluster CLUSTER = RULE.getMesosCluster();

    private static final DockerClient DOCKERCLIENT = DockerClientFactory.build();

    private static final MesosClusterContainersFactory FACTORY = new MesosClusterContainersFactory();

    @BeforeClass
    public static void reinitialiseCluster() {
        IpTables.apply(DOCKERCLIENT, CLUSTER, TEST_CONFIG);
    }

    @BeforeClass
    public static void prepareCleanDockerEnvironment() {
        new DockerUtil(DOCKERCLIENT).killAllSchedulers();
        new DockerUtil(DOCKERCLIENT).killAllExecutors();

        // Completely wipe out old data dir, just in case there is any old data in there.
        String dataDir = org.apache.mesos.elasticsearch.scheduler.Configuration.DEFAULT_HOST_DATA_DIR;

        AlpineContainer alpineContainer = new AlpineContainer(DOCKERCLIENT, dataDir, dataDir, "sh", "-c", "rm -rf " + dataDir + "/* ; sleep 9999 ;");
        alpineContainer.start(TEST_CONFIG.getClusterTimeout());
        alpineContainer.remove();
    }

    @AfterClass
    public static void killAllContainers() {
        new DockerUtil(DOCKERCLIENT).killAllExecutors();
    }

    public static Configuration getTestConfig() {
        return TEST_CONFIG;
    }

    public static DockerClient getDockerClient() {
        return DOCKERCLIENT;
    }
}
