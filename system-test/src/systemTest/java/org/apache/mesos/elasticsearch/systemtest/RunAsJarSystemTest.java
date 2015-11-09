package org.apache.mesos.elasticsearch.systemtest;

import com.containersol.minimesos.MesosCluster;
import com.containersol.minimesos.container.AbstractContainer;
import com.containersol.minimesos.mesos.*;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.Link;
import com.github.dockerjava.api.model.Ports;
import org.apache.mesos.elasticsearch.common.cli.ElasticsearchCLIParameter;
import org.apache.mesos.elasticsearch.common.cli.ZookeeperCLIParameter;
import org.apache.mesos.elasticsearch.scheduler.Configuration;
import org.apache.mesos.elasticsearch.systemtest.callbacks.ElasticsearchNodesResponse;
import org.apache.mesos.elasticsearch.systemtest.util.DockerUtil;
import org.junit.*;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.stream.IntStream;

import static org.apache.mesos.elasticsearch.systemtest.Configuration.getDocker0AdaptorIpAddress;
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
    protected static final org.apache.mesos.elasticsearch.systemtest.Configuration TEST_CONFIG = new org.apache.mesos.elasticsearch.systemtest.Configuration();
    private static final int NUMBER_OF_TEST_TASKS = 1;

    // Need full control over the cluster, so need to do all the lifecycle stuff.
    private MesosCluster cluster;
    private static DockerClient dockerClient = DockerClientFactory.build();
    private JarScheduler scheduler;

    @BeforeClass
    public static void prepareCleanDockerEnvironment() {
        new DockerUtil(dockerClient).killAllSchedulers();
        new DockerUtil(dockerClient).killAllExecutors();
    }

    @Rule
    public final TestWatcher WATCHER = new TestWatcher() {
        @Override
        protected void failed(Throwable e, Description description) {
            cluster.stop();
        }
    };

    @Before
    public void before() {
        ClusterArchitecture.Builder builder = new ClusterArchitecture.Builder()
                .withZooKeeper()
                .withMaster()
                .withContainer(zkContainer -> new JarScheduler(dockerClient, zkContainer), ClusterContainers.Filter.zooKeeper());
        scheduler = (JarScheduler) builder.getContainers().getOne(container -> container instanceof JarScheduler).get();
        IntStream.range(0, NUMBER_OF_TEST_TASKS).forEach(dummy ->
            builder.withSlave(zooKeeper -> new MesosSlaveWithSchedulerLink(dockerClient, zooKeeper, scheduler))
        );
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
    public void shouldStartScheduler() throws IOException, InterruptedException {
        ESTasks esTasks = new ESTasks(TEST_CONFIG, scheduler.getIpAddress());
        new TasksResponse(esTasks, NUMBER_OF_TEST_TASKS);

        ElasticsearchNodesResponse nodesResponse = new ElasticsearchNodesResponse(esTasks, NUMBER_OF_TEST_TASKS);
        assertTrue("Elasticsearch nodes did not discover each other within 5 minutes", nodesResponse.isDiscoverySuccessful());
    }

    private static class JarScheduler extends AbstractContainer {
        private final ZooKeeper zooKeeperContainer;
        private String docker0AdaptorIpAddress;

        protected JarScheduler(DockerClient dockerClient, ZooKeeper zooKeeperContainer) {
            super(dockerClient);
            this.zooKeeperContainer = zooKeeperContainer;
            docker0AdaptorIpAddress = getDocker0AdaptorIpAddress(dockerClient);
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
                    .withExtraHosts("hostnamehack:" + docker0AdaptorIpAddress) // Will redirect hostnamehack to host IP address (where ports are bound)
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

    private static  class MesosSlaveWithSchedulerLink extends MesosSlave {

        private final JarScheduler scheduler;

        protected MesosSlaveWithSchedulerLink(DockerClient dockerClient, ZooKeeper zooKeeperContainer, JarScheduler scheduler) {
            super(dockerClient, zooKeeperContainer);
            this.scheduler = scheduler;
        }

        @Override
        protected CreateContainerCmd dockerCommand() {
            CreateContainerCmd createContainerCmd = super.dockerCommand();

            Ports ports = new Ports();
            ports.bind(ExposedPort.tcp(31000), Ports.Binding(31000));
            createContainerCmd
                    .withHostName("hostnamehack") // Hack to get past offer refusal
                    .withLinks(new Link(scheduler.getContainerId(), scheduler.getContainerId()))
                    .withExposedPorts(new ExposedPort(31000), new ExposedPort(5051))
                    .withPortBindings(ports)
                    ;
            return createContainerCmd;
        }
    }
}
