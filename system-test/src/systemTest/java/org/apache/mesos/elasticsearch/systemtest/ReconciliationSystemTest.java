package org.apache.mesos.elasticsearch.systemtest;

import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.model.Container;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.apache.mesos.mini.MesosCluster;
import org.apache.mesos.mini.container.AbstractContainer;
import org.apache.mesos.mini.mesos.MesosClusterConfig;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.jayway.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;

/**
 * Tests cluster state monitoring and reconciliation.
 */
@SuppressWarnings({"PMD.TooManyMethods"})
public class ReconciliationSystemTest {
    private static final Logger LOGGER = Logger.getLogger(ReconciliationSystemTest.class);
    private static final int CLUSTER_SIZE = 3;
    private static final int TIMEOUT = 60;
    private static final String MESOS_LOCAL_IMAGE_NAME = "mesos-local";

    private static final ContainerLifecycleManagement CONTAINER_MANGER = new ContainerLifecycleManagement();
    private static final MesosClusterConfig CONFIG = MesosClusterConfig.builder()
            .numberOfSlaves(CLUSTER_SIZE)
            .privateRegistryPort(15000) // Currently you have to choose an available port by yourself
            .slaveResources(new String[]{"ports(*):[9200-9200,9300-9300]", "ports(*):[9201-9201,9301-9301]", "ports(*):[9202-9202,9302-9302]"})
            .build();
    @ClassRule
    private static final MesosCluster CLUSTER = new MesosCluster(CONFIG);

    private static String mesosClusterId;

    @BeforeClass
    public static void beforeScheduler() throws Exception {
        LOGGER.debug("Injecting executor");
        CLUSTER.injectImage("mesos/elasticsearch-executor");
        await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> CONFIG.dockerClient.listContainersCmd().exec().size() == CLUSTER_SIZE);
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

        String id = clusterKillOne();
        LOGGER.debug("Deleted " + id);

        // Should restart an executor, so there should still be three
        assertCorrectNumberOfExecutors();

    }

    private void assertCorrectNumberOfExecutors() throws IOException {
        await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> clusterPs().size() == CLUSTER_SIZE);
        List<String> result = clusterPs();
        LOGGER.debug("Mini-mesos PS command = " + Arrays.toString(result.toArray()));
        assertEquals(CLUSTER_SIZE, result.size());
    }

    private String clusterKillOne() throws IOException {
        String executorId = getLastExecutor();
        ExecCreateCmdResponse exec = CONFIG.dockerClient.execCreateCmd(mesosClusterId).withAttachStdout().withAttachStderr().withCmd("docker", "kill", executorId).exec();
        InputStream execCmdStream = CONFIG.dockerClient.execStartCmd(exec.getId()).exec();
        return IOUtils.toString(execCmdStream, "UTF-8");
    }

    // Note: we cant use the task response again because the tasks are only added when created.
    private List<String> clusterPs() throws IOException {
        ExecCreateCmdResponse exec = CONFIG.dockerClient.execCreateCmd(mesosClusterId).withAttachStdout().withAttachStderr().withCmd("docker", "ps", "-q").exec();
        InputStream execCmdStream = CONFIG.dockerClient.execStartCmd(exec.getId()).exec();
        String result = IOUtils.toString(execCmdStream, "UTF-8");
        return Arrays.asList(result.replaceAll("[^0-9a-zA-Z\n.]", "").split("\n"));
    }

    private String getLastExecutor() throws IOException {
        return clusterPs().get(0);
    }

    private static ElasticsearchSchedulerContainer startSchedulerContainer() {
        ElasticsearchSchedulerContainer scheduler = new ElasticsearchSchedulerContainer(CONFIG.dockerClient, CLUSTER.getMesosContainer().getIpAddress());
        CONTAINER_MANGER.addAndStart(scheduler);
        return scheduler;
    }

    /**
     * Simple class to monitor lifecycle of scheduler container.
     */
    private static class ContainerLifecycleManagement {
        private List<AbstractContainer> containers = new ArrayList<>();
        public void addAndStart(AbstractContainer container) {
            container.start();
            containers.add(container);
        }

        public void stopContainer(AbstractContainer container) {
            container.remove();
            containers.remove(container);
        }

        public void stopAll() {
            containers.forEach(AbstractContainer::remove);
            containers.clear();
        }
    }

}
