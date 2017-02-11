package org.apache.mesos.elasticsearch.systemtest;

import com.containersol.minimesos.cluster.MesosCluster;
import com.containersol.minimesos.config.ContainerConfigBlock;
import com.containersol.minimesos.container.AbstractContainer;
import com.containersol.minimesos.docker.DockerClientFactory;
import com.containersol.minimesos.junit.MesosClusterTestRule;
import com.containersol.minimesos.mesos.MesosClusterContainersFactory;
import com.containersol.minimesos.mesos.ZooKeeperContainer;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.model.AccessMode;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.Volume;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.log4j.Logger;
import org.apache.mesos.elasticsearch.common.cli.ElasticsearchCLIParameter;
import org.apache.mesos.elasticsearch.common.cli.ZookeeperCLIParameter;
import org.apache.mesos.elasticsearch.common.zookeeper.ZooKeeper;
import org.apache.mesos.elasticsearch.systemtest.base.SchedulerTestBase;
import org.apache.mesos.elasticsearch.systemtest.callbacks.ElasticsearchNodesResponse;
import org.apache.mesos.elasticsearch.systemtest.containers.AlpineContainer;
import org.apache.mesos.elasticsearch.systemtest.containers.MesosSlaveTagged;
import org.apache.mesos.elasticsearch.systemtest.util.DockerUtil;
import org.apache.mesos.elasticsearch.systemtest.util.IpTables;
import org.json.JSONObject;
import org.junit.*;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.mesos.elasticsearch.scheduler.Configuration.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test that custom settings work
 */
@Ignore("This test has to be merged into DeploymentSystemTest. See https://github.com/mesos/elasticsearch/issues/591")
public class CustomSettingsDockerSystemTest {
    public static final String CUSTOM_YML = "elasticsearch.yml";
    public static final String CUSTOM_CONFIG_PATH = "/tmp/config/"; // In the container and on the VM/Host
    public static final String CUSTOM_CONFIG_FILE = "/tmp/config/" + CUSTOM_YML; // In the container and on the VM/Host
    public static final String TEST_PATH_PLUGINS = "/tmp/plugins";
    public static final String TEST_AUTO_EXPAND_REPLICAS = "false";
    private static final Logger LOGGER = Logger.getLogger(SchedulerTestBase.class);
    private static final DockerClient dockerClient = DockerClientFactory.build();
    private DockerUtil dockerUtil = new DockerUtil(dockerClient);
    private static MesosClusterContainersFactory factory = new MesosClusterContainersFactory();

    @ClassRule
    public static final MesosClusterTestRule RULE = MesosClusterTestRule.fromFile("src/systemTest/resources/testMinimesosFile");

    public static final MesosCluster CLUSTER = RULE.getMesosCluster();

    // Need full control over the cluster, so need to do all the lifecycle stuff.
    protected static final org.apache.mesos.elasticsearch.systemtest.Configuration TEST_CONFIG = new org.apache.mesos.elasticsearch.systemtest.Configuration();

    @BeforeClass
    public static void startCluster() {
        IpTables.apply(dockerClient, CLUSTER, TEST_CONFIG);
    }

    @AfterClass
    public static void killAllContainers() {
        new DockerUtil(dockerClient).killAllSchedulers();
        new DockerUtil(dockerClient).killAllExecutors();
    }

    @After
    public void after() {
        dockerUtil.killAllSchedulers();
        dockerUtil.killAllExecutors();
    }

    @Test
    public void shouldHaveCustomSettingsBasedOnPath() throws UnirestException {
        final AlpineContainer ymlWrite = new AlpineContainer(dockerClient, CUSTOM_CONFIG_PATH, CUSTOM_CONFIG_PATH,
                "sh", "-c", "echo \"index.auto_expand_replicas: " + TEST_AUTO_EXPAND_REPLICAS + "\npath.plugins: " + TEST_PATH_PLUGINS + "\" > " + CUSTOM_CONFIG_FILE);
        ymlWrite.start(TEST_CONFIG.getClusterTimeout());
        ymlWrite.remove();

        LOGGER.info("Starting Elasticsearch scheduler");

        AbstractContainer scheduler = new CustomSettingsScheduler(dockerClient, (ZooKeeperContainer) CLUSTER.getZooKeeper(), CLUSTER.getClusterId(), CUSTOM_CONFIG_FILE);
        CLUSTER.addAndStartProcess(scheduler, TEST_CONFIG.getClusterTimeout());

        LOGGER.info("Started Elasticsearch scheduler on " + scheduler.getIpAddress() + ":" + TEST_CONFIG.getSchedulerGuiPort());

        ESTasks esTasks = new ESTasks(TEST_CONFIG, scheduler.getIpAddress());
        new TasksResponse(esTasks, TEST_CONFIG.getElasticsearchNodesCount());

        ElasticsearchNodesResponse nodesResponse = new ElasticsearchNodesResponse(esTasks, TEST_CONFIG.getElasticsearchNodesCount());
        assertTrue("Elasticsearch nodes did not discover each other within 5 minutes", nodesResponse.isDiscoverySuccessful());

        final JSONObject root = Unirest.get("http://" + esTasks.getEsHttpAddressList().get(0) + "/_nodes").asJson().getBody().getObject();
        final JSONObject nodes = root.getJSONObject("nodes");
        final String firstNode = nodes.keys().next();

        // Test a setting that is not specified by the framework (to test that it is written correctly)
        final String pathPlugins = nodes.getJSONObject(firstNode).getJSONObject("settings").getJSONObject("path").getString("plugins");
        assertEquals(TEST_PATH_PLUGINS, pathPlugins);

        // Test a setting that is specified by the framework (to test it is overwritten correctly)
        final String autoExpandReplicas = nodes.getJSONObject(firstNode).getJSONObject("settings").getJSONObject("index").getString("auto_expand_replicas");
        assertEquals(TEST_AUTO_EXPAND_REPLICAS, autoExpandReplicas);
    }

    @Test
    public void shouldHaveCustomSettingsBasedOnURL() throws UnirestException {
        LOGGER.info("Starting Elasticsearch scheduler");

        String url = "https://gist.githubusercontent.com/philwinder/afece65f5560f1f7e1a2/raw/64dfca8cf76253de3185013b92697c7aea72bf5f/elasticsearch.yml";
        AbstractContainer scheduler = new CustomSettingsScheduler(dockerClient, (ZooKeeperContainer) CLUSTER.getZooKeeper(), CLUSTER.getClusterId(), url);
        CLUSTER.addAndStartProcess(scheduler, TEST_CONFIG.getClusterTimeout());

        LOGGER.info("Started Elasticsearch scheduler on " + scheduler.getIpAddress() + ":" + TEST_CONFIG.getSchedulerGuiPort());

        ESTasks esTasks = new ESTasks(TEST_CONFIG, scheduler.getIpAddress());
        new TasksResponse(esTasks, TEST_CONFIG.getElasticsearchNodesCount());

        ElasticsearchNodesResponse nodesResponse = new ElasticsearchNodesResponse(esTasks, TEST_CONFIG.getElasticsearchNodesCount());
        assertTrue("Elasticsearch nodes did not discover each other within 5 minutes", nodesResponse.isDiscoverySuccessful());

        final JSONObject root = Unirest.get("http://" + esTasks.getEsHttpAddressList().get(0) + "/_nodes").asJson().getBody().getObject();
        final JSONObject nodes = root.getJSONObject("nodes");
        final String firstNode = nodes.keys().next();

        // Test a setting that is not specified by the framework (to test that it is written correctly)
        final String pathPlugins = nodes.getJSONObject(firstNode).getJSONObject("settings").getJSONObject("path").getString("plugins");
        assertEquals(TEST_PATH_PLUGINS, pathPlugins);

        // Test a setting that is specified by the framework (to test it is overwritten correctly)
        final String autoExpandReplicas = nodes.getJSONObject(firstNode).getJSONObject("settings").getJSONObject("index").getString("auto_expand_replicas");
        assertEquals(TEST_AUTO_EXPAND_REPLICAS, autoExpandReplicas);
    }

    private static class CustomSettingsScheduler extends AbstractContainer {
        private final ZooKeeperContainer zooKeeperContainer;
        private final String clusterId;
        private final String configPath;

        protected CustomSettingsScheduler(DockerClient dockerClient, ZooKeeperContainer zooKeeperContainer, String clusterId, String configPath) {
            super(new ContainerConfigBlock(TEST_CONFIG.getSchedulerImageName(), "latest"));
            this.zooKeeperContainer = zooKeeperContainer;
            this.clusterId = clusterId;
            this.configPath = configPath;
        }

        @Override
        public void pullImage() {
            dockerClient.pullImageCmd(TEST_CONFIG.getSchedulerImageName());
        }

        @Override
        protected CreateContainerCmd dockerCommand() {
            return dockerClient
                    .createContainerCmd(TEST_CONFIG.getSchedulerImageName())
                    .withName(TEST_CONFIG.getSchedulerName() + "_" + clusterId + "_" + new SecureRandom().nextInt())
                    .withBinds(new Bind(CUSTOM_CONFIG_PATH, new Volume(CUSTOM_CONFIG_PATH), AccessMode.ro))
                    .withEnv("JAVA_OPTS=-Xms128m -Xmx256m")
                    .withCmd(
                            ElasticsearchCLIParameter.ELASTICSEARCH_SETTINGS_LOCATION, configPath,
                            ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, getZookeeperMesosUrl(),
                            ELASTICSEARCH_CPU, "0.25",
                            ELASTICSEARCH_RAM, "256",
                            ELASTICSEARCH_DISK, "10",
                            USE_IP_ADDRESS, "true"
                    );
        }

        public String getZookeeperMesosUrl() {
            return "zk://" + zooKeeperContainer.getIpAddress() + ":2181/mesos";
        }

        @Override
        public String getRole() {
            return TEST_CONFIG.getSchedulerName();
        }
    }
//
//    /**
//     * Adds a volume to the sandbox location
//     */
//    private static class MesosSlaveWithVolume extends MesosSlaveTagged {
//        public static final String MESOS_SANDBOX = "/tmp/mesos/slaves";
//
//        public MesosSlaveWithVolume(ZooKeeper zooKeeperContainer, String resources) {
//            super(resources);
//        }
//
//        @Override
//        protected CreateContainerCmd dockerCommand() {
//            final CreateContainerCmd command = super.dockerCommand();
//            List<Bind> binds = new ArrayList<>();
//            binds.addAll(Arrays.asList(command.getBinds()));
//            binds.add(new Bind(MESOS_SANDBOX, new Volume(MESOS_SANDBOX), AccessMode.rw));
//            binds.add(new Bind(CUSTOM_CONFIG_PATH, new Volume(CUSTOM_CONFIG_PATH), AccessMode.ro));
//            return command.withBinds(binds.toArray(new Bind[binds.size()]));
//        }
//    }
}
