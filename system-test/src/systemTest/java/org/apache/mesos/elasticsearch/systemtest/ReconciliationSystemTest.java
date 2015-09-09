package org.apache.mesos.elasticsearch.systemtest;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.DockerClientConfig;
import org.apache.log4j.Logger;
import org.apache.mesos.mini.MesosCluster;
import org.apache.mesos.mini.mesos.MesosClusterConfig;
import org.hamcrest.CoreMatchers;
import org.json.JSONObject;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.jayway.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Tests cluster state monitoring and reconciliation.
 */
@SuppressWarnings({"PMD.TooManyMethods"})
public class ReconciliationSystemTest {
    private static final Logger LOGGER = Logger.getLogger(ReconciliationSystemTest.class);
    private static final int CLUSTER_SIZE = 3;
    private static final int TIMEOUT = 60;
    private static final String MESOS_LOCAL_IMAGE_NAME = "mesos-local";
    public static final int DOCKER_PORT = 2376;

    private static final ContainerLifecycleManagement CONTAINER_MANGER = new ContainerLifecycleManagement();
    private static final MesosClusterConfig CONFIG = MesosClusterConfig.builder()
            .numberOfSlaves(CLUSTER_SIZE)
            .privateRegistryPort(15000) // Currently you have to choose an available port by yourself
            .slaveResources(new String[]{"ports(*):[9200-9200,9300-9300]", "ports(*):[9201-9201,9301-9301]", "ports(*):[9202-9202,9302-9302]"})
            .build();
    @ClassRule
    public static final MesosCluster CLUSTER = new MesosCluster(CONFIG);

    private static String mesosClusterId;
    private static DockerClient innerDockerClient;

    @BeforeClass
    public static void beforeScheduler() throws Exception {
        String innerDockerHost;

        LOGGER.debug("Local docker environment");
        innerDockerHost = CLUSTER.getMesosContainer().getIpAddress() + ":" + DOCKER_PORT;

        DockerClientConfig.DockerClientConfigBuilder dockerConfigBuilder = DockerClientConfig.createDefaultConfigBuilder().withUri("http://" + innerDockerHost);

        innerDockerClient = DockerClientBuilder.getInstance(dockerConfigBuilder.build()).build();

        LOGGER.debug("Injecting executor");
        CLUSTER.injectImage("mesos/elasticsearch-executor");
        await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> CONFIG.dockerClient.listContainersCmd().exec().size() > 0); // Wait until mesos-local has started.
        List<Container> containers = CONFIG.dockerClient.listContainersCmd().exec();

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

    @After
    public void after() {
        CONTAINER_MANGER.stopAll();
    }

    @Test
    public void ifSchedulerLostShouldReconcileExecutors() throws IOException {
        ElasticsearchSchedulerContainer scheduler = startSchedulerContainer();
        assertCorrectNumberOfExecutors();

        // Stop and restart container
        CONTAINER_MANGER.stopContainer(scheduler);
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
        CONTAINER_MANGER.stopContainer(scheduler);

        // Kill executor
        killOneExecutor();

        startSchedulerContainer();
        // Should restart an executor, so there should still be three
        assertCorrectNumberOfExecutors();
    }

    @Test
    public void canPickUpStateAfterSchedulerIsDead() throws IOException {
        ElasticsearchSchedulerContainer schedulerBeforeReconcilliation = startSchedulerContainer();
        assertCorrectNumberOfExecutors();
        TasksResponse tasksBeforeReconcilliation = new TasksResponse(schedulerBeforeReconcilliation.getIpAddress(), CLUSTER_SIZE);
        assertEquals(3, tasksBeforeReconcilliation.getJson().getBody().getArray().length());
        for (JSONObject task : tasksBeforeReconcilliation.getTasks()) {
            assertThat(task.getString("http_address"), CoreMatchers.startsWith("172.17."));
            assertThat(task.getString("transport_address"), CoreMatchers.startsWith("172.17."));
            assertThat(task.getString("hostname"), CoreMatchers.startsWith("slave"));
        }

        // Kill scheduler
        CONTAINER_MANGER.stopContainer(schedulerBeforeReconcilliation);

        ElasticsearchSchedulerContainer schedulerAfterReconcilliation = startSchedulerContainer();
        // Should restart an executor, so there should still be three
        assertCorrectNumberOfExecutors();

        TasksResponse tasksAfterReconcilliation = new TasksResponse(schedulerAfterReconcilliation.getIpAddress(), CLUSTER_SIZE);
        for (JSONObject task : tasksAfterReconcilliation.getTasks()) {
            assertThat(task.getString("http_address"), CoreMatchers.startsWith("172.17.0"));
            assertThat(task.getString("transport_address"), CoreMatchers.startsWith("172.17.0"));
        }
        assertEquals(3, tasksAfterReconcilliation.getJson().getBody().getArray().length());
    }

    private void assertCorrectNumberOfExecutors() throws IOException {
        await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> clusterPs().size() == CLUSTER_SIZE);
        List<Container> result = clusterPs();
        LOGGER.debug("Mini-mesos PS command = " + Arrays.toString(result.stream().map(Container::getId).collect(Collectors.toList()).toArray()));
        assertEquals(CLUSTER_SIZE, result.size());
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

    private static ElasticsearchSchedulerContainer startSchedulerContainer() {
        ElasticsearchSchedulerContainer scheduler = new ElasticsearchSchedulerContainer(CONFIG.dockerClient, CLUSTER.getMesosContainer().getIpAddress());
        CONTAINER_MANGER.addAndStart(scheduler);
        return scheduler;
    }

}
