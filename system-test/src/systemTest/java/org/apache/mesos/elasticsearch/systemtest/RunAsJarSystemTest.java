package org.apache.mesos.elasticsearch.systemtest;

import com.containersol.minimesos.cluster.MesosCluster;
import com.containersol.minimesos.container.AbstractContainer;
import com.containersol.minimesos.mesos.ClusterArchitecture;
import com.containersol.minimesos.mesos.ClusterContainers;
import com.containersol.minimesos.mesos.DockerClientFactory;
import com.containersol.minimesos.mesos.ZooKeeper;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.model.*;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.mesos.elasticsearch.common.cli.ElasticsearchCLIParameter;
import org.apache.mesos.elasticsearch.common.cli.ZookeeperCLIParameter;
import org.apache.mesos.elasticsearch.scheduler.Configuration;
import org.apache.mesos.elasticsearch.systemtest.callbacks.ElasticsearchNodesResponse;
import org.apache.mesos.elasticsearch.systemtest.containers.AlpineContainer;
import org.apache.mesos.elasticsearch.systemtest.containers.MesosMasterTagged;
import org.apache.mesos.elasticsearch.systemtest.containers.MesosSlaveTagged;
import org.apache.mesos.elasticsearch.systemtest.util.DockerUtil;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.stream.IntStream;

import static org.apache.mesos.elasticsearch.systemtest.Configuration.getDocker0AdaptorIpAddress;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * A system test to ensure that the framework can run as a JAR, not using docker.
 * <p>
 * <b>Test method</b>
 * <p>
 *     The goal is to start the framework in jar mode, and ensure that the system can still be discovered.
 *     We still start the scheduler in the container (but with jar mode enabled) because the system running the system
 *     test may not have Java 8 installed. But this is no issue; the test is exactly the same.
 * </p><p>
 *     Next, a custom MesosSlave is provided so that it can resolve the IP address of the scheduler (which is hosting
 *     the executor jar). Its own hostname is set to "hostnamehack" so that it can be resolved on the scheduler side.
 *     The ES port (31000 in this case) is bound to the host, where the host is the VM or linux OS. This will allow us
 *     to curl the ES endpoint without knowing the IP address of the slave container.
 * </p><p>
 *     The scheduler has an extra host called "hostnamehack" that resolves to the docker0 adaptor address. This is
 *     equivalent to the VM/linux localhost, is known ahead of time and is system independent. I.e. both Mac's and linux
 *     boxes will be able to resolve this address. The port is the same.
 * </p><p>
 *     So finally, the ES system tests can cary on as normal and resolve the ES hosts. Nice!
 * </p>
 */
@SuppressWarnings({"PMD.TooManyMethods"})
public class RunAsJarSystemTest {
    private static final Logger LOG = LoggerFactory.getLogger(RunAsJarSystemTest.class);
    protected static final org.apache.mesos.elasticsearch.systemtest.Configuration TEST_CONFIG = new org.apache.mesos.elasticsearch.systemtest.Configuration();
    private static final int NUMBER_OF_TEST_TASKS = 3;
    public static final String CUSTOM_YML = "elasticsearch.yml";
    public static final String CUSTOM_CONFIG_PATH = "/tmp/config/"; // In the container and on the VM/Host
    public static final String CUSTOM_CONFIG_FILE = "/tmp/config/" + CUSTOM_YML; // In the container and on the VM/Host
    public static final String TEST_PATH_HOME = "./test";
    public static final String TEST_PATH_PLUGINS = "./TESTPLUGINS";
    public static final String TEST_AUTO_EXPAND_REPLICAS = "false";

    // Need full control over the cluster, so need to do all the lifecycle stuff.
    private static MesosCluster cluster;
    private static DockerClient dockerClient = DockerClientFactory.build();
    private static JarScheduler scheduler;
    private static ESTasks esTasks;
    private static ElasticsearchNodesResponse nodesResponse;

    @Rule
    public final TestWatcher watcher = new TestWatcher() {
        @Override
        protected void failed(Throwable e, Description description) {
            cluster.stop();
        }
    };

    @BeforeClass
    public static void startCluster() {
        new DockerUtil(dockerClient).killAllSchedulers();
        new DockerUtil(dockerClient).killAllExecutors();

        final AlpineContainer ymlWrite = new AlpineContainer(dockerClient, CUSTOM_CONFIG_PATH, CUSTOM_CONFIG_PATH,
                "sh", "-c", "echo \"index.auto_expand_replicas: " + TEST_AUTO_EXPAND_REPLICAS + "\npath.plugins: " + TEST_PATH_PLUGINS + "\" > " + CUSTOM_CONFIG_FILE);
        ymlWrite.start(10);
        ymlWrite.remove();

        ClusterArchitecture.Builder builder = new ClusterArchitecture.Builder()
                .withZooKeeper()
                .withMaster(MesosMasterTagged::new)
                .withContainer(zkContainer -> new JarScheduler(dockerClient, zkContainer, zkContainer.getClusterId()), ClusterContainers.Filter.zooKeeper());
        scheduler = (JarScheduler) builder.getContainers().getOne(container -> container instanceof JarScheduler).get();
        IntStream.range(0, NUMBER_OF_TEST_TASKS).forEach(number ->
                        builder.withSlave(zooKeeper -> new MesosSlaveWithSchedulerLink(zooKeeper, scheduler, number))
        );
        cluster = new MesosCluster(builder.build());

        cluster.start(TEST_CONFIG.getClusterTimeout());

        // Make sure all tasks are running before we test.
        esTasks = new ESTasks(TEST_CONFIG, scheduler.getIpAddress());
        new TasksResponse(esTasks, NUMBER_OF_TEST_TASKS);

        nodesResponse = new ElasticsearchNodesResponse(esTasks, NUMBER_OF_TEST_TASKS);
    }

    @AfterClass
    public static void killAllContainers() {
        cluster.stop();
        new DockerUtil(dockerClient).killAllSchedulers();
        new DockerUtil(dockerClient).killAllExecutors();
    }

    @Test
    public void shouldStartScheduler() throws IOException, InterruptedException {
        assertTrue("Elasticsearch nodes did not discover each other within 5 minutes", nodesResponse.isDiscoverySuccessful());
    }

    @Test
    public void shouldCluster() throws IOException, InterruptedException, UnirestException {
        final int numberOfNodes = Unirest.get("http://" + esTasks.getEsHttpAddressList().get(0) + "/_cluster/health").asJson().getBody().getObject().getInt("number_of_nodes");
        assertEquals("Elasticsearch nodes did not discover each other within 5 minutes", NUMBER_OF_TEST_TASKS, numberOfNodes);
    }

    @Test
    public void shouldHaveCustomSettingsBasedOnPath() throws UnirestException {
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

    private static class JarScheduler extends AbstractContainer {
        private final ZooKeeper zooKeeperContainer;
        private final String clusterId;
        private String docker0AdaptorIpAddress;

        protected JarScheduler(DockerClient dockerClient, ZooKeeper zooKeeperContainer, String clusterId) {
            super(dockerClient);
            this.zooKeeperContainer = zooKeeperContainer;
            this.clusterId = clusterId;
            docker0AdaptorIpAddress = getDocker0AdaptorIpAddress();
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
                    .withEnv("JAVA_OPTS=-Xms128m -Xmx256m")
                    .withExtraHosts("hostnamehack:" + docker0AdaptorIpAddress) // Will redirect hostnamehack to host IP address (where ports are bound)
                    .withCmd(
                            ElasticsearchCLIParameter.ELASTICSEARCH_SETTINGS_LOCATION, CUSTOM_CONFIG_FILE,
                            ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, getZookeeperMesosUrl(),
                            Configuration.FRAMEWORK_USE_DOCKER, "false",
                            ElasticsearchCLIParameter.ELASTICSEARCH_NODES, String.valueOf(NUMBER_OF_TEST_TASKS),
                            Configuration.ELASTICSEARCH_CPU, "0.1",
                            Configuration.ELASTICSEARCH_RAM, "128",
                            Configuration.ELASTICSEARCH_DISK, "10",
                            Configuration.JAVA_HOME, "/usr/bin"
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

    private static class MesosSlaveWithSchedulerLink extends MesosSlaveTagged {

        private final JarScheduler scheduler;
        private final Integer slaveNumber;

        protected MesosSlaveWithSchedulerLink(ZooKeeper zooKeeperContainer, JarScheduler scheduler, Integer slaveNumber) {
            super(zooKeeperContainer, "ports(*):[" + httpPort(slaveNumber) + "-" + httpPort(slaveNumber) + "," + transportPort(slaveNumber) + "-" + transportPort(slaveNumber) + "]; cpus(*):0.2; mem(*):256; disk(*):200");
            this.scheduler = scheduler;
            this.slaveNumber = slaveNumber;
        }

        private static Integer httpPort(Integer slaveNumber) {
            return 31000 + slaveNumber;
        }

        private static Integer transportPort(Integer slaveNumber) {
            return 31100 + slaveNumber;
        }

        @Override
        protected CreateContainerCmd dockerCommand() {
            CreateContainerCmd createContainerCmd = super.dockerCommand();

            Ports ports = new Ports();
            ports.bind(ExposedPort.tcp(httpPort(slaveNumber)), Ports.Binding(httpPort(slaveNumber)));
            createContainerCmd
                    .withHostName("hostnamehack") // Hack to get past offer refusal
                    .withLinks(new Link(scheduler.getContainerId(), scheduler.getContainerId()))
                    .withExposedPorts(new ExposedPort(httpPort(slaveNumber)), new ExposedPort(5051))
                    .withBinds(new Bind(CUSTOM_CONFIG_PATH, new Volume(CUSTOM_CONFIG_PATH))) // For custom config
                    .withPortBindings(ports)
                    ;
            return createContainerCmd;
        }
    }
}
