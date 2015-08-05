package org.apache.mesos.elasticsearch.systemtest;

import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.model.Container;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.apache.mesos.mini.MesosCluster;
import org.apache.mesos.mini.mesos.MesosClusterConfig;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.jayway.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;

/**
 * Tests cluster state monitoring and reconcilliation.
 */
public class ReconcilliationTest {
    protected static final MesosClusterConfig config = MesosClusterConfig.builder()
            .numberOfSlaves(3)
            .privateRegistryPort(15000) // Currently you have to choose an available port by yourself
            .slaveResources(new String[]{"ports(*):[9200-9200,9300-9300]", "ports(*):[9201-9201,9301-9301]", "ports(*):[9202-9202,9302-9302]"})
            .build();

    private static final Logger LOGGER = Logger.getLogger(ReconcilliationTest.class);

    @ClassRule
    public static final MesosCluster cluster = new MesosCluster(config);
    private static String mesosClusterId;

    @BeforeClass
    public static void beforeScheduler() throws Exception {
        cluster.injectImage("mesos/elasticsearch-executor");
        await().atMost(60, TimeUnit.SECONDS).until(() -> config.dockerClient.listContainersCmd().exec().size() == 3);
        List<Container> containers = config.dockerClient.listContainersCmd().exec();

        // Find a single executor container
        mesosClusterId = "";
        for (Container container : containers) {
            if (container.getImage().contains("mesos-local")) {
                mesosClusterId = container.getId();
                break;
            }
        }
    }

    @Test
    public void ifSchedulerLostShouldReconcileExecutors() throws IOException {
        ElasticsearchSchedulerContainer scheduler = startSchedulerContainer();
        await().atMost(60, TimeUnit.SECONDS).until(() -> clusterPs().size() == 3);
        List<String> result = clusterPs();
        assertEquals(3, result.size());

        // Stop and restart container
        scheduler.remove();
        scheduler = startSchedulerContainer();
        await().atMost(60, TimeUnit.SECONDS).until(() -> clusterPs().size() == 3);
        result = clusterPs();
        assertEquals(3, result.size());
        scheduler.remove();
    }

    @Test
    public void ifExecutorIsLostShouldStartAnother() throws IOException {
        ElasticsearchSchedulerContainer scheduler = startSchedulerContainer();
        await().atMost(60, TimeUnit.SECONDS).until(() -> clusterPs().size() == 3);
        List<String> result = clusterPs();
        assertEquals(3, result.size());

        await().atMost(60, TimeUnit.SECONDS).until(() -> clusterPs().size() == 3);
        result = clusterPs();
        assertEquals(3, result.size());

        String another = clusterKillOne();

        LOGGER.debug("Deleted " + another);

        // Should restart an executor, so there should still be three
        await().atMost(60, TimeUnit.SECONDS).until(() -> clusterPs().size() == 3);
        result = clusterPs();
        assertEquals(3, result.size());
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
        return Arrays.asList(result.split("\n"));
    }

    private String getLastExecutor() throws IOException {
        return clusterPs().get(0).replaceAll("[^0-9a-zA-Z.]", "");
    }

    private static ElasticsearchSchedulerContainer startSchedulerContainer() {
        ElasticsearchSchedulerContainer scheduler = new ElasticsearchSchedulerContainer(config.dockerClient, cluster.getMesosContainer().getIpAddress());
        scheduler.start();
        return scheduler;
    }

}
