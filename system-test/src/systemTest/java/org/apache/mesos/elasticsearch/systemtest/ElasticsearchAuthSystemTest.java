package org.apache.mesos.elasticsearch.systemtest;

import com.containersol.minimesos.cluster.MesosCluster;
import com.containersol.minimesos.docker.DockerClientFactory;
import com.containersol.minimesos.junit.MesosClusterTestRule;
import com.containersol.minimesos.mesos.MesosClusterContainersFactory;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.model.AccessMode;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.Volume;
import org.apache.mesos.elasticsearch.common.cli.ElasticsearchCLIParameter;
import org.apache.mesos.elasticsearch.common.cli.ZookeeperCLIParameter;
import org.apache.mesos.elasticsearch.systemtest.callbacks.ElasticsearchNodesResponse;
import org.apache.mesos.elasticsearch.systemtest.containers.ElasticsearchSchedulerContainer;
import org.apache.mesos.elasticsearch.systemtest.util.DockerUtil;
import org.apache.mesos.elasticsearch.systemtest.util.IpTables;
import org.junit.*;

import java.io.FileNotFoundException;

import static org.junit.Assert.assertTrue;

/**
 * Test Auth.
 * This currently tests authentication and roles. It does not test ACL's yet.
 */
@Ignore("This test has to be merged into DeploymentSystemTest. See https://github.com/mesos/elasticsearch/issues/591")
public class ElasticsearchAuthSystemTest {
    protected static final Configuration TEST_CONFIG = new Configuration();
    public static final String FRAMEWORKPASSWD = "frameworkpasswd";
    public static final String SECRET = "mesospasswd";
    public static final String SECRET_FOLDER = "/tmp/test/";
    public static final String ALPINE = "alpine";

    private static DockerClient dockerClient = DockerClientFactory.build();
    private static MesosClusterContainersFactory factory = new MesosClusterContainersFactory();

    @ClassRule
    public static final MesosClusterTestRule RULE = MesosClusterTestRule.fromFile("src/systemTest/resources/testMinimesosFile-authentication");

    public static final MesosCluster CLUSTER = RULE.getMesosCluster();

    @BeforeClass
    public static void prepareCleanDockerEnvironment() {
        new DockerUtil(dockerClient).killAllSchedulers();
        new DockerUtil(dockerClient).killAllExecutors();
    }

    @Before
    public void before() throws FileNotFoundException {
        writePasswordFileToVM();

        IpTables.apply(dockerClient, CLUSTER, TEST_CONFIG);
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
        if (CLUSTER != null) {
            CLUSTER.destroy(factory);
        }
    }

    @AfterClass
    public static void killAllContainers() {
        new DockerUtil(dockerClient).killAllSchedulers();
        new DockerUtil(dockerClient).killAllExecutors();
    }

    @Test
    public void shouldStartFrameworkWithRole() {
        SmallerCPUScheduler scheduler = new SmallerCPUScheduler(dockerClient, CLUSTER.getZooKeeper().getIpAddress(), "testRole", CLUSTER);
        CLUSTER.addAndStartProcess(scheduler, TEST_CONFIG.getClusterTimeout());

        ESTasks esTasks = new ESTasks(TEST_CONFIG, scheduler.getIpAddress());
        new TasksResponse(esTasks, TEST_CONFIG.getElasticsearchNodesCount());

        ElasticsearchNodesResponse nodesResponse = new ElasticsearchNodesResponse(esTasks, TEST_CONFIG.getElasticsearchNodesCount());
        assertTrue("Elasticsearch nodes did not discover each other within 5 minutes", nodesResponse.isDiscoverySuccessful());
    }

    // TODO (Frank) Add this to minimesosFile

//    /**
//     * Only the role "testRole" can start frameworks.
//     */
//    private static class SimpleAuthMaster extends MesosMasterContainer {
//
//        public SimpleAuthMaster(MesosCluster cluster, String uuid, String containerId) {
//            super(cluster, uuid, containerId);
//        }
//
//
//        private static TreeMap<String, String> envVars() {
//            TreeMap<String, String> envVars = new TreeMap<>();
//            envVars.put("MESOS_ROLES", "testRole");
//            envVars.put("MESOS_CREDENTIALS",  SECRET_FOLDER + SECRET);
//            envVars.put("MESOS_AUTHENTICATE", "true");
//            return envVars;
//        }
//
//        @Override
//        protected CreateContainerCmd dockerCommand() {
//            CreateContainerCmd createContainerCmd = super.dockerCommand();
//            createContainerCmd.withBinds(new Bind(SECRET_FOLDER, new Volume(SECRET_FOLDER)));
//            return createContainerCmd;
//        }
//    }
//
//    private static class SimpleAuthSlave extends MesosSlaveTagged {
//
//        protected SimpleAuthSlave(ZooKeeper zooKeeperContainer, String resources) {
//            super(resources);
//        }
//    }
//
    private static class SmallerCPUScheduler extends ElasticsearchSchedulerContainer {

        public SmallerCPUScheduler(DockerClient dockerClient, String zkIp, String frameworkRole, MesosCluster cluster) {
            super(dockerClient, zkIp, frameworkRole, org.apache.mesos.elasticsearch.scheduler.Configuration.DEFAULT_HOST_DATA_DIR);
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