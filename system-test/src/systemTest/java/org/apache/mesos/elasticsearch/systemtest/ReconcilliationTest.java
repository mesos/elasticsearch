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
public class ReconcilliationTest {
    private static final Logger LOGGER = Logger.getLogger(ReconcilliationTest.class);
    public static final int CLUSTER_SIZE = 3;
    public static final int TIMEOUT = 60;
    public static final String MESOS_LOCAL_IMAGE_NAME = "mesos-local";

    private static final ContainerLifecycleManagement containerManger = new ContainerLifecycleManagement();
    protected static final MesosClusterConfig config = MesosClusterConfig.builder()
            .numberOfSlaves(CLUSTER_SIZE)
            .privateRegistryPort(15000) // Currently you have to choose an available port by yourself
            .slaveResources(new String[]{"ports(*):[9200-9200,9300-9300]", "ports(*):[9201-9201,9301-9301]", "ports(*):[9202-9202,9302-9302]"})
            .build();
    @ClassRule
    public static final MesosCluster cluster = new MesosCluster(config);

    private static String mesosClusterId;

    @BeforeClass
    public static void beforeScheduler() throws Exception {
        LOGGER.debug("Injecting executor");
        cluster.injectImage("mesos/elasticsearch-executor");
        await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> config.dockerClient.listContainersCmd().exec().size() == CLUSTER_SIZE);
        List<Container> containers = config.dockerClient.listContainersCmd().exec();

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
        containerManger.stopAll();
    }

    @Test
    public void ifSchedulerLostShouldReconcileExecutors() throws IOException {
        ElasticsearchSchedulerContainer scheduler = startSchedulerContainer();
        assertCorrectNumberOfExecutors();

        // Stop and restart container
        containerManger.stopContainer(scheduler);
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
        ExecCreateCmdResponse exec = config.dockerClient.execCreateCmd(mesosClusterId).withAttachStdout().withAttachStderr().withCmd("docker", "kill", executorId).exec();
        InputStream execCmdStream = config.dockerClient.execStartCmd(exec.getId()).exec();
        return IOUtils.toString(execCmdStream, "UTF-8");
    }

    // Note: we cant use the task response again because the tasks are only added when created.
    private List<String> clusterPs() throws IOException {
        ExecCreateCmdResponse exec = config.dockerClient.execCreateCmd(mesosClusterId).withAttachStdout().withAttachStderr().withCmd("docker", "ps", "-q").exec();
        InputStream execCmdStream = config.dockerClient.execStartCmd(exec.getId()).exec();
        String result = IOUtils.toString(execCmdStream, "UTF-8");
        return Arrays.asList(result.replaceAll("[^0-9a-zA-Z\n.]", "").split("\n"));
    }

    private String getLastExecutor() throws IOException {
        return clusterPs().get(0);
    }

    private static ElasticsearchSchedulerContainer startSchedulerContainer() {
        ElasticsearchSchedulerContainer scheduler = new ElasticsearchSchedulerContainer(config.dockerClient, cluster.getMesosContainer().getIpAddress());
        containerManger.addAndStart(scheduler);
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
