package org.apache.mesos.elasticsearch.systemtest;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.DockerClientConfig;
import org.apache.log4j.Logger;
import org.apache.mesos.elasticsearch.common.cli.ElasticsearchCLIParameter;
import org.apache.mesos.elasticsearch.common.cli.ZookeeperCLIParameter;
import org.apache.mesos.elasticsearch.scheduler.Configuration;
import org.apache.mesos.mini.MesosCluster;
import org.apache.mesos.mini.mesos.MesosClusterConfig;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.jayway.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests cluster state monitoring and reconciliation.
 */
@SuppressWarnings({"PMD.TooManyMethods"})
public class ReconciliationSystemTest {
    public static final int DOCKER_PORT = 2376;
    private static final Logger LOGGER = Logger.getLogger(ReconciliationSystemTest.class);
    private static final org.apache.mesos.elasticsearch.systemtest.Configuration TEST_CONFIG = new org.apache.mesos.elasticsearch.systemtest.Configuration();

    private static final int TIMEOUT = 60;
    private static final String MESOS_LOCAL_IMAGE_NAME = "mesos-local";
    private static final ContainerLifecycleManagement CONTAINER_MANAGER = new ContainerLifecycleManagement();
    private static String mesosClusterId;
    private static DockerClient innerDockerClient;

    @ClassRule
    public static final MesosCluster CLUSTER = new MesosCluster(
        MesosClusterConfig.builder()
            .numberOfSlaves(TEST_CONFIG.getElasticsearchNodesCount())
            .privateRegistryPort(TEST_CONFIG.getPrivateRegistryPort()) // Currently you have to choose an available port by yourself
            .slaveResources(TEST_CONFIG.getPortRanges())
            .build()
    );

    @BeforeClass
    public static void beforeScheduler() throws Exception {
        String innerDockerHost;

        LOGGER.debug("Local docker environment");
        innerDockerHost = CLUSTER.getMesosContainer().getIpAddress() + ":" + DOCKER_PORT;

        DockerClientConfig.DockerClientConfigBuilder dockerConfigBuilder = DockerClientConfig.createDefaultConfigBuilder().withUri("http://" + innerDockerHost);

        innerDockerClient = DockerClientBuilder.getInstance(dockerConfigBuilder.build()).build();

        LOGGER.debug("Injecting executor");
        CLUSTER.injectImage(TEST_CONFIG.getExecutorImageName());
        await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> CLUSTER.getConfig().dockerClient.listContainersCmd().exec().size() > 0); // Wait until mesos-local has started.
        List<Container> containers = CLUSTER.getConfig().dockerClient.listContainersCmd().exec();

        // Find the mesos-local container so we can do docker in docker commands.
        mesosClusterId = "";
        for (Container container : containers) {
            if (container.getImage().contains(MESOS_LOCAL_IMAGE_NAME)) {
                mesosClusterId = container.getId();
                break;
            }
        }
        LOGGER.debug("Mini-mesos container ID: " + mesosClusterId);
    }

    private static ElasticsearchSchedulerContainer startSchedulerContainer() {
        ElasticsearchSchedulerContainer scheduler = new ElasticsearchSchedulerContainer(CLUSTER.getConfig().dockerClient, CLUSTER.getMesosContainer().getIpAddress());
        CONTAINER_MANAGER.addAndStart(scheduler);
        return scheduler;
    }

    @After
    public void after() {
        CONTAINER_MANAGER.stopAll();
    }

    @Test
    public void forceCheckExecutorTimeout() throws IOException {
        ElasticsearchSchedulerContainer scheduler = new TimeoutSchedulerContainer(CLUSTER.getConfig().dockerClient, CLUSTER.getMesosContainer().getIpAddress());
        CONTAINER_MANAGER.addAndStart(scheduler);
        assertCorrectNumberOfExecutors(); // Start with 3
        assertLessThan(TEST_CONFIG.getElasticsearchNodesCount()); // Then should be less than 3, because at some point we kill an executor
        assertCorrectNumberOfExecutors(); // Then at some point should get back to 3.
    }

    @Test
    public void ifSchedulerLostShouldReconcileExecutors() throws IOException {
        ElasticsearchSchedulerContainer scheduler = startSchedulerContainer();
        assertCorrectNumberOfExecutors();

        // Stop and restart container
        CONTAINER_MANAGER.stopContainer(scheduler);
        startSchedulerContainer();
        assertCorrectNumberOfExecutors();
    }

    @Test
    public void ifExecutorIsLostShouldStartAnother() throws IOException {
        startSchedulerContainer();
        assertCorrectNumberOfExecutors();

        killOneExecutor();

        // Should restart an executor, so there should still be three
        assertCorrectNumberOfExecutors();

    }

    @Test
    public void ifExecutorIsLostWhileSchedulerIsDead() throws IOException {
        ElasticsearchSchedulerContainer scheduler = startSchedulerContainer();
        assertCorrectNumberOfExecutors();

        // Kill scheduler
        CONTAINER_MANAGER.stopContainer(scheduler);

        // Kill executor
        killOneExecutor();

        startSchedulerContainer();
        // Should restart an executor, so there should still be three
        assertCorrectNumberOfExecutors();
    }

    private void assertCorrectNumberOfExecutors() throws IOException {
        assertCorrectNumberOfExecutors(TEST_CONFIG.getElasticsearchNodesCount());
    }

    private void assertLessThan(int expected) throws IOException {
        await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> clusterPs().size() < expected);
        List<Container> result = getContainers();
        assertTrue(result.size() < expected);
    }

    private List<Container> getContainers() throws IOException {
        List<Container> result = clusterPs();
        LOGGER.debug("Mini-mesos PS command = " + Arrays.toString(result.stream().map(Container::getId).collect(Collectors.toList()).toArray()));
        return result;
    }

    private void assertCorrectNumberOfExecutors(int expected) throws IOException {
        await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> clusterPs().size() == expected);
        List<Container> result = getContainers();
        assertEquals(expected, result.size());
    }

    private void killOneExecutor() throws IOException {
        String executorId = getLastExecutor();
        LOGGER.debug("Killing container: " + executorId);
        innerDockerClient.killContainerCmd(executorId).exec();
    }

    // Note: we cant use the task response again because the tasks are only added when created.
    private List<Container> clusterPs() throws IOException {
        return innerDockerClient.listContainersCmd().exec();
    }

    private String getLastExecutor() throws IOException {
        return clusterPs().get(0).getId();
    }

    private static class TimeoutSchedulerContainer extends ElasticsearchSchedulerContainer {
        protected TimeoutSchedulerContainer(DockerClient dockerClient, String mesosIp) {
            super(dockerClient, mesosIp);
        }

        @Override
        protected CreateContainerCmd dockerCommand() {
            return dockerClient
                    .createContainerCmd(TEST_CONFIG.getSchedulerImageName())
                    .withName(TEST_CONFIG.getSchedulerName() + "_" + new SecureRandom().nextInt())
                    .withEnv("JAVA_OPTS=-Xms128m -Xmx256m")
                    .withExtraHosts(IntStream.rangeClosed(1, 3).mapToObj(value -> "slave" + value + ":" + mesosIp).toArray(String[]::new))
                    .withCmd(
                            ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, getZookeeperMesosUrl(),
                            Configuration.EXECUTOR_HEALTH_DELAY, "99",
                            Configuration.EXECUTOR_TIMEOUT, "100", // This timeout is valid, but will always timeout, because of delays in receiving healthchecks.
                            ElasticsearchCLIParameter.ELASTICSEARCH_NODES, "3",
                            Configuration.ELASTICSEARCH_RAM, "256"
                    );
        }
    }

}
