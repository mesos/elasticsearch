package org.apache.mesos.elasticsearch.systemtest;

import com.containersol.minimesos.MesosCluster;
import com.containersol.minimesos.container.AbstractContainer;
import com.containersol.minimesos.mesos.*;
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
import org.apache.mesos.elasticsearch.systemtest.util.DockerUtil;
import org.json.JSONObject;
import org.junit.*;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.TreeMap;
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

        final AlpineContainer ymlWrite = new AlpineContainer(dockerClient, CUSTOM_CONFIG_PATH, CUSTOM_CONFIG_PATH, "sh", "-c", "echo \"path.home: " + TEST_PATH_HOME + "\" > " + CUSTOM_CONFIG_FILE); // TODO test external config here
        ymlWrite.start(10);
        ymlWrite.remove();

        ClusterArchitecture.Builder builder = new ClusterArchitecture.Builder()
                .withZooKeeper()
                .withMaster()
                .withContainer(zkContainer -> new JarScheduler(dockerClient, zkContainer, zkContainer.getClusterId()), ClusterContainers.Filter.zooKeeper());
        scheduler = (JarScheduler) builder.getContainers().getOne(container -> container instanceof JarScheduler).get();
        IntStream.range(0, NUMBER_OF_TEST_TASKS).forEach(number ->
                        builder.withSlave(zooKeeper -> new MesosSlaveWithSchedulerLink(dockerClient, zooKeeper, scheduler, number))
        );
        cluster = new MesosCluster(builder.build());

        cluster.start(TEST_CONFIG.getClusterTimeout());

        // Make sure all tasks are running before we test.
        esTasks = new ESTasks(TEST_CONFIG, scheduler.getIpAddress(), true);
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
        final int number_of_nodes = Unirest.get("http://" + esTasks.getEsHttpAddressList().get(0) + "/_cluster/health").asJson().getBody().getObject().getInt("number_of_nodes");
        assertEquals("Elasticsearch nodes did not discover each other within 5 minutes", NUMBER_OF_TEST_TASKS, number_of_nodes);
    }

    @Ignore // TODO (PNW): Reinstate this test when the config is hosted by the Scheduler
    @Test
    public void shouldHaveCustomSettingsBasedOnPath() throws UnirestException {
        final JSONObject root = Unirest.get("http://" + esTasks.getEsHttpAddressList().get(0) + "/_nodes").asJson().getBody().getObject();
        final JSONObject nodes = root.getJSONObject("nodes");
        final String firstNode = nodes.keys().next();
        final String pathHome = nodes.getJSONObject(firstNode).getJSONObject("settings").getJSONObject("path").getString("home");
        assertEquals(TEST_PATH_HOME, pathHome);
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
    }

    private static  class MesosSlaveWithSchedulerLink extends MesosSlave {

        private final JarScheduler scheduler;
        private final Integer httpPort;
        private final Integer transportPort;

        protected MesosSlaveWithSchedulerLink(DockerClient dockerClient, ZooKeeper zooKeeperContainer, JarScheduler scheduler, Integer slaveNumber) {
            super(dockerClient, zooKeeperContainer);
            this.scheduler = scheduler;
            httpPort = 31000 + slaveNumber;
            transportPort = 31100 + slaveNumber;
        }

        @Override
        protected CreateContainerCmd dockerCommand() {
            CreateContainerCmd createContainerCmd = super.dockerCommand();

            Ports ports = new Ports();
            ports.bind(ExposedPort.tcp(httpPort), Ports.Binding(httpPort));
            createContainerCmd
                    .withHostName("hostnamehack") // Hack to get past offer refusal
                    .withLinks(new Link(scheduler.getContainerId(), scheduler.getContainerId()))
                    .withExposedPorts(new ExposedPort(httpPort), new ExposedPort(5051))
                    .withBinds(new Bind(CUSTOM_CONFIG_PATH, new Volume(CUSTOM_CONFIG_PATH))) // For custom config
                    .withPortBindings(ports)
                    ;
            return createContainerCmd;
        }

        @Override
        public TreeMap<String, String> getDefaultEnvVars() {
            final TreeMap<String, String> defaultEnvVars = super.getDefaultEnvVars();
            defaultEnvVars.put("MESOS_RESOURCES", "ports(*):[" + httpPort + "-" + httpPort + "," + transportPort + "-" + transportPort + "]; cpus(*):0.2; mem(*):256; disk(*):200");
            return defaultEnvVars;
        }
    }
}
