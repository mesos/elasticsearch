package org.apache.mesos.elasticsearch.systemtest;

import com.containersol.minimesos.MesosCluster;
import com.containersol.minimesos.container.AbstractContainer;
import com.containersol.minimesos.mesos.*;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.Link;
import com.github.dockerjava.api.model.Ports;
import org.apache.log4j.Logger;
import org.apache.mesos.elasticsearch.common.cli.ElasticsearchCLIParameter;
import org.apache.mesos.elasticsearch.common.cli.ZookeeperCLIParameter;
import org.apache.mesos.elasticsearch.scheduler.Configuration;
import org.apache.mesos.elasticsearch.systemtest.callbacks.ElasticsearchNodesResponse;
import org.apache.mesos.elasticsearch.systemtest.util.ContainerLifecycleManagement;
import org.apache.mesos.elasticsearch.systemtest.util.DockerUtil;
import org.junit.*;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.io.File;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.TreeMap;
import java.util.stream.IntStream;

import static org.junit.Assert.assertTrue;

/**
 * A system test to ensure that the framework can run as a JAR, not using docker.
 */
public class RunAsJarSystemTest {
    private static final Logger LOGGER = Logger.getLogger(RunAsJarSystemTest.class);
    private static final ContainerLifecycleManagement CONTAINER_MANAGER = new ContainerLifecycleManagement();
    protected static final org.apache.mesos.elasticsearch.systemtest.Configuration TEST_CONFIG = new org.apache.mesos.elasticsearch.systemtest.Configuration();
    private static final int NUMBER_OF_TEST_TASKS = 1;

    // Need full control over the cluster, so need to do all the lifecycle stuff.
    private static MesosCluster CLUSTER;
    private static DockerClient dockerClient = DockerClientFactory.build();
    private JarScheduler scheduler;

    @Before
    public void before() {
        ClusterArchitecture.Builder builder = new ClusterArchitecture.Builder()
                .withZooKeeper()
                .withMaster(zooKeeper -> new MesosMaster22(dockerClient, zooKeeper))
                .withSlave(zooKeeper -> new MesosSlave22(dockerClient, zooKeeper)) // Have to have a slave before we build. This offer wont get accepted.
                .withContainer(zkContainer -> new JarScheduler(dockerClient, zkContainer), ClusterContainers.Filter.zooKeeper());
        scheduler = (JarScheduler) builder.build().getClusterContainers().getOne(container -> container instanceof JarScheduler).get();
        IntStream.range(0,NUMBER_OF_TEST_TASKS).forEach(dummy ->
            builder.withSlave(zooKeeper -> new MesosSlaveWithSchedulerLink(dockerClient, zooKeeper, scheduler))
        );
        CLUSTER = new MesosCluster(builder.build());

        CLUSTER.start();

    }

    @After
    public void stopContainer() {
        CONTAINER_MANAGER.stopAll();
        CLUSTER.stop();
    }

    @ClassRule
    public static final TestWatcher WATCHER = new TestWatcher() {
        @Override
        protected void failed(Throwable e, Description description) {
            CLUSTER.stop();
        }
    };

    @BeforeClass
    public static void prepareCleanDockerEnvironment() {
        new DockerUtil(dockerClient).killAllSchedulers();
        new DockerUtil(dockerClient).killAllExecutors();
    }

    @AfterClass
    public static void killAllContainers() {
        new DockerUtil(dockerClient).killAllExecutors();
    }

    @Test
    public void shouldStartScheduler() throws IOException, InterruptedException {
        ESTasks esTasks = new ESTasks(TEST_CONFIG, scheduler.getIpAddress());
        new TasksResponse(esTasks, NUMBER_OF_TEST_TASKS);

        ElasticsearchNodesResponse nodesResponse = new ElasticsearchNodesResponse(esTasks, NUMBER_OF_TEST_TASKS);
        assertTrue("Elasticsearch nodes did not discover each other within 5 minutes", nodesResponse.isDiscoverySuccessful());
    }

    private class JarScheduler extends AbstractContainer {
        private final ZooKeeper zooKeeperContainer;

        protected JarScheduler(DockerClient dockerClient, ZooKeeper zooKeeperContainer) {
            super(dockerClient);
            this.zooKeeperContainer = zooKeeperContainer;
        }

        @Override
        public void pullImage() {
            dockerClient.pullImageCmd(TEST_CONFIG.getSchedulerImageName());
        }

        @Override
        protected CreateContainerCmd dockerCommand() {
            return dockerClient
                    .createContainerCmd(TEST_CONFIG.getSchedulerImageName())
                    .withName(TEST_CONFIG.getSchedulerName() + "_" + MesosCluster.getClusterId() + "_" + new SecureRandom().nextInt())
                    .withEnv("JAVA_OPTS=-Xms128m -Xmx256m")
                    .withExtraHosts("hostnamehack:" + ElasticsearchSchedulerContainer.DOCKER0_ADAPTOR_IP_ADDRESS) // Hack to get past offer refusal
                    .withCmd(
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

    private class MesosMaster22 extends MesosMaster {

        public MesosMaster22(DockerClient dockerClient, ZooKeeper zooKeeperContainer) {
            super(dockerClient, zooKeeperContainer);
        }

        @Override
        protected CreateContainerCmd dockerCommand() {
            CreateContainerCmd createContainerCmd = super.dockerCommand();
            return createContainerCmd.withImage("containersol/mesos-master:" + Main.MESOS_IMAGE_TAG);
        }
    }

    private class MesosSlave22 extends MesosSlave {

        public MesosSlave22(DockerClient dockerClient, ZooKeeper zooKeeperContainer) {
            super(dockerClient, zooKeeperContainer);
        }

        @Override
        protected CreateContainerCmd dockerCommand() {
            CreateContainerCmd createContainerCmd = super.dockerCommand();
            return createContainerCmd.withImage("containersol/mesos-agent:" + Main.MESOS_IMAGE_TAG);
        }
    }

    private class MesosSlaveWithSchedulerLink extends MesosSlave {

        private final JarScheduler scheduler;

        protected MesosSlaveWithSchedulerLink(DockerClient dockerClient, ZooKeeper zooKeeperContainer, JarScheduler scheduler) {
            super(dockerClient, zooKeeperContainer);
            this.scheduler = scheduler;
        }

        @Override
        protected CreateContainerCmd dockerCommand() {
            String dockerBin1 = "/usr/bin/docker";
            File dockerBinFile1 = new File(dockerBin1);
            if(!dockerBinFile1.exists() || !dockerBinFile1.canExecute()) {
                dockerBin1 = "/usr/local/bin/docker";
                dockerBinFile1 = new File(dockerBin1);
                if(!dockerBinFile1.exists() || !dockerBinFile1.canExecute()) {
                    LOGGER.error("Docker binary not found in /usr/local/bin or /usr/bin. Creating containers will most likely fail.");
                }
            }
            Ports ports = new Ports();
            ports.bind(ExposedPort.tcp(31000), Ports.Binding(31000));
            CreateContainerCmd createContainerCmd = this.dockerClient
                    .createContainerCmd("containersol/mesos-agent:" + Main.MESOS_IMAGE_TAG)
                    .withName("minimesos-agent-" + MesosCluster.getClusterId() + "-" + this.getRandomId())
                    .withPrivileged(true)
                    .withEnv(this.createMesosLocalEnvironment())
                    .withPid("host")
                    .withBinds(new Bind[]{Bind.parse("/var/lib/docker:/var/lib/docker"), Bind.parse("/sys/fs/cgroup:/sys/fs/cgroup"), Bind.parse(String.format("%s:/usr/bin/docker", new Object[]{dockerBin1})), Bind.parse("/var/run/docker.sock:/var/run/docker.sock")})
                    .withHostName("hostnamehack") // Hack to get past offer refusal
                    .withNetworkMode("bridge")
                    .withLinks(new Link(scheduler.getContainerId(), scheduler.getContainerId()))
                    .withExposedPorts(new ExposedPort(31000), new ExposedPort(5051))
                    .withPortBindings(ports)
                    ;
            return createContainerCmd;
        }

        @Override
        protected String[] createMesosLocalEnvironment() {
            TreeMap<String, String> map = this.getDefaultEnvVars();
            map.putAll(this.getSharedEnvVars());
            return (String[])map.entrySet().stream().map((e) -> {
                return (String)e.getKey() + "=" + (String)e.getValue();
            }).toArray((x$0) -> {
                return new String[x$0];
            });
        }
    }
}
