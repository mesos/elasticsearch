package org.apache.mesos.elasticsearch.systemtest;

import com.containersol.minimesos.MesosCluster;
import com.containersol.minimesos.mesos.*;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.model.AccessMode;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.Volume;
import org.apache.mesos.elasticsearch.common.cli.ElasticsearchCLIParameter;
import org.apache.mesos.elasticsearch.common.cli.ZookeeperCLIParameter;
import org.apache.mesos.elasticsearch.systemtest.callbacks.ElasticsearchNodesResponse;
import org.apache.mesos.elasticsearch.systemtest.util.DockerUtil;
import org.junit.*;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.io.FileNotFoundException;
import java.util.TreeMap;

import static org.junit.Assert.assertTrue;

/**
 * Test Auth.
 * This currently tests authentication and roles. It does not test ACL's yet.
 */
public class ElasticsearchAuthSystemTest {
    protected static final Configuration TEST_CONFIG = new Configuration();
    public static final String FRAMEWORKPASSWD = "frameworkpasswd";
    public static final String SECRET = "mesospasswd";
    public static final String SECRET_FOLDER = "/tmp/test/";
    public static final String ALPINE = "alpine";
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
    public void before() throws FileNotFoundException {
        writePasswordFileToVM();
        ClusterArchitecture.Builder builder = new ClusterArchitecture.Builder()
                .withZooKeeper()
                .withMaster(zooKeeper -> new SimpleAuthMaster(dockerClient, zooKeeper))
                .withSlave(zooKeeper -> new SimpleAuthSlave(dockerClient, zooKeeper, "ports(testRole):[9200-9200,9300-9300]; cpus(testRole):0.2; mem(testRole):256; disk(testRole):200"))
                .withSlave(zooKeeper -> new SimpleAuthSlave(dockerClient, zooKeeper, "ports(testRole):[9201-9201,9301-9301]; cpus(testRole):0.2; mem(testRole):256; disk(testRole):200"))
                .withSlave(zooKeeper -> new SimpleAuthSlave(dockerClient, zooKeeper, "ports(testRole):[9202-9202,9302-9302]; cpus(testRole):0.2; mem(testRole):256; disk(testRole):200"));
        cluster = new MesosCluster(builder.build());
        cluster.start();
    }

    /**
     * Use an alpine image to write files to the VM/docker host. Then all the containers can mount that DIR and get access to the passwd files.
     */
    private void writePasswordFileToVM() {
        // Mesos password
        CreateContainerResponse exec = dockerClient.createContainerCmd(ALPINE)
                .withBinds(new Bind(SECRET_FOLDER, new Volume(SECRET_FOLDER), AccessMode.rw))
                .withCmd("rm", "-r", SECRET_FOLDER)
                .exec();
        dockerClient.startContainerCmd(exec.getId()).exec();
        exec = dockerClient.createContainerCmd(ALPINE)
                .withBinds(new Bind(SECRET_FOLDER, new Volume(SECRET_FOLDER), AccessMode.rw))
                .withCmd("sh", "-c", "echo -n testRole secret | tee -a " + SECRET_FOLDER + SECRET)
                .exec();
        dockerClient.startContainerCmd(exec.getId()).exec();

        // Framework password
        // Note that the definition is slightly different. There is no username specified in the file. Just the password.
        exec = dockerClient.createContainerCmd(ALPINE)
                .withBinds(new Bind(SECRET_FOLDER, new Volume(SECRET_FOLDER), AccessMode.rw))
                .withCmd("sh", "-c", "echo -n secret | tee -a " + SECRET_FOLDER + FRAMEWORKPASSWD)
                .exec();
        dockerClient.startContainerCmd(exec.getId()).exec();
    }

    @After
    public void stopContainer() {
        if (cluster != null) {
            cluster.stop();
        }
    }

    @AfterClass
    public static void killAllContainers() {
        new DockerUtil(dockerClient).killAllSchedulers();
        new DockerUtil(dockerClient).killAllExecutors();
    }

    @Test
    public void shouldStartFrameworkWithRole() {
        SmallerCPUScheduler scheduler = new SmallerCPUScheduler(dockerClient, cluster.getZkContainer().getIpAddress(), "testRole", cluster);
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
            envVars.put("MESOS_CREDENTIALS",  SECRET_FOLDER + SECRET);
            envVars.put("MESOS_AUTHENTICATE", "true");
            return envVars;
        }

        @Override
        protected CreateContainerCmd dockerCommand() {
            CreateContainerCmd createContainerCmd = super.dockerCommand();
            createContainerCmd.withBinds(new Bind(SECRET_FOLDER, new Volume(SECRET_FOLDER)));
            return createContainerCmd;
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
            super(dockerClient, zkIp, frameworkRole, cluster, org.apache.mesos.elasticsearch.scheduler.Configuration.DEFAULT_HOST_DATA_DIR);
        }

        @Override
        protected CreateContainerCmd dockerCommand() {
            CreateContainerCmd createContainerCmd = super.dockerCommand();
            createContainerCmd.withBinds(new Bind(SECRET_FOLDER, new Volume(SECRET_FOLDER)));
            createContainerCmd.withCmd(
                    ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, getZookeeperMesosUrl(),
                    ElasticsearchCLIParameter.ELASTICSEARCH_NODES, Integer.toString(TEST_CONFIG.getElasticsearchNodesCount()),
                    org.apache.mesos.elasticsearch.scheduler.Configuration.ELASTICSEARCH_RAM, "256",
                    org.apache.mesos.elasticsearch.scheduler.Configuration.ELASTICSEARCH_DISK, "10",
                    org.apache.mesos.elasticsearch.scheduler.Configuration.USE_IP_ADDRESS, "true",
                    org.apache.mesos.elasticsearch.scheduler.Configuration.FRAMEWORK_ROLE, "testRole",
                    org.apache.mesos.elasticsearch.scheduler.Configuration.FRAMEWORK_PRINCIPAL, "testRole",
                    org.apache.mesos.elasticsearch.scheduler.Configuration.FRAMEWORK_SECRET_PATH, SECRET_FOLDER + FRAMEWORKPASSWD,
                    org.apache.mesos.elasticsearch.scheduler.Configuration.ELASTICSEARCH_CPU, "0.2"
            );
            return createContainerCmd;
        }
    }
}