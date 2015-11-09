package org.apache.mesos.elasticsearch.systemtest;

import com.containersol.minimesos.MesosCluster;
import com.containersol.minimesos.mesos.*;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import org.apache.log4j.Logger;
import org.apache.mesos.elasticsearch.common.cli.ElasticsearchCLIParameter;
import org.apache.mesos.elasticsearch.common.cli.ZookeeperCLIParameter;
import org.apache.mesos.elasticsearch.systemtest.callbacks.ElasticsearchNodesResponse;
import org.apache.mesos.elasticsearch.systemtest.util.DockerUtil;
import org.junit.*;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.util.TreeMap;

import static org.junit.Assert.assertTrue;

/**
 * Test Auth.
 */
public class ElasticsearchAuthSystemTest {
    protected static final Configuration TEST_CONFIG = new Configuration();
    private MesosCluster cluster;
    private static DockerClient dockerClient = DockerClientFactory.build();

    @BeforeClass
    public static void prepareCleanDockerEnvironment() {
        new DockerUtil(dockerClient).killAllSchedulers();
        new DockerUtil(dockerClient).killAllExecutors();
    }

    @Rule
    public final TestWatcher watcher = new TestWatcher() {
        @Override
        protected void failed(Throwable e, Description description) {
            cluster.stop();
        }
    };

    @Before
    public void before() {
        ClusterArchitecture.Builder builder = new ClusterArchitecture.Builder()
                .withZooKeeper()
                .withMaster(zooKeeper -> new SimpleAuthMaster(dockerClient, zooKeeper))
                .withSlave(zooKeeper -> new SimpleAuthSlave(dockerClient, zooKeeper, "ports(testRole):[9200-9200,9300-9300]; cpus(testRole):0.2; mem(testRole):256; disk(testRole):200"))
                .withSlave(zooKeeper -> new SimpleAuthSlave(dockerClient, zooKeeper, "ports(testRole):[9201-9201,9301-9301]; cpus(testRole):0.2; mem(testRole):256; disk(testRole):200"))
                .withSlave(zooKeeper -> new SimpleAuthSlave(dockerClient, zooKeeper, "ports(testRole):[9202-9202,9302-9302]; cpus(testRole):0.2; mem(testRole):256; disk(testRole):200"));
        cluster = new MesosCluster(builder.build());
        cluster.start();
    }

    @After
    public void stopContainer() {
        cluster.stop();
    }

    @AfterClass
    public static void killAllContainers() {
        new DockerUtil(dockerClient).killAllSchedulers();
        new DockerUtil(dockerClient).killAllExecutors();
    }

    @Test
    public void shouldStartFrameworkWithRole() {
        ElasticsearchSchedulerContainer scheduler = new SmallerCPUScheduler(dockerClient, cluster.getZkContainer().getIpAddress(), "testRole", cluster);
        cluster.addAndStartContainer(scheduler);

        ESTasks esTasks = new ESTasks(TEST_CONFIG, scheduler.getIpAddress());
        new TasksResponse(esTasks, TEST_CONFIG.getElasticsearchNodesCount());

        ElasticsearchNodesResponse nodesResponse = new ElasticsearchNodesResponse(esTasks, TEST_CONFIG.getElasticsearchNodesCount());
        assertTrue("Elasticsearch nodes did not discover each other within 5 minutes", nodesResponse.isDiscoverySuccessful());
    }


    /**
     * Only the role "testRole" can start frameworks.
     */
    private static class SimpleAuthMaster extends MesosMaster {

        protected SimpleAuthMaster(DockerClient dockerClient, ZooKeeper zooKeeperContainer) {
            super(dockerClient, zooKeeperContainer);
        }

        @Override
        public TreeMap<String, String> getDefaultEnvVars() {
            TreeMap<String, String> envVars = super.getDefaultEnvVars();
            envVars.put("MESOS_ROLES", "testRole");
            return envVars;
        }
    }

    private static class SimpleAuthSlave extends MesosSlave {
        private final String resources;

        protected SimpleAuthSlave(DockerClient dockerClient, ZooKeeper zooKeeperContainer, String resources) {
            super(dockerClient, zooKeeperContainer);
            this.resources = resources;
        }

        @Override
        public TreeMap<String, String> getDefaultEnvVars() {
            TreeMap<String, String> envVars = super.getDefaultEnvVars();
            envVars.remove("MESOS_RESOURCES");
            envVars.put("MESOS_RESOURCES", resources);
            return envVars;
        }
    }

    private static class SmallerCPUScheduler extends ElasticsearchSchedulerContainer {

        public SmallerCPUScheduler(DockerClient dockerClient, String zkIp, String frameworkRole, MesosCluster cluster) {
            super(dockerClient, zkIp, frameworkRole, cluster);
        }

        @Override
        protected CreateContainerCmd dockerCommand() {
            CreateContainerCmd createContainerCmd = super.dockerCommand();
            createContainerCmd.withCmd(
                    ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, getZookeeperMesosUrl(),
                    ElasticsearchCLIParameter.ELASTICSEARCH_NODES, Integer.toString(TEST_CONFIG.getElasticsearchNodesCount()),
                    org.apache.mesos.elasticsearch.scheduler.Configuration.ELASTICSEARCH_RAM, "256",
                    org.apache.mesos.elasticsearch.scheduler.Configuration.ELASTICSEARCH_DISK, "10",
                    org.apache.mesos.elasticsearch.scheduler.Configuration.FRAMEWORK_ROLE, "testRole",
                    org.apache.mesos.elasticsearch.scheduler.Configuration.ELASTICSEARCH_CPU, "0.2"
            );
            return createContainerCmd;
        }
    }
}