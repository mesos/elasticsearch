package org.apache.mesos.elasticsearch.systemtest;

import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.command.StartContainerCmd;
import com.jayway.awaitility.Awaitility;
import org.apache.mesos.elasticsearch.common.cli.ElasticsearchCLIParameter;
import org.apache.mesos.elasticsearch.common.cli.ZookeeperCLIParameter;
import org.apache.mesos.elasticsearch.scheduler.Configuration;
import org.apache.mesos.elasticsearch.systemtest.base.TestBase;
import org.apache.mesos.elasticsearch.systemtest.callbacks.LogContainerTestCallback;
import org.apache.mesos.elasticsearch.systemtest.util.DockerUtil;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

/**
 * Tests the main method.
 */
public class SchedulerMainSystemTest extends TestBase {

    @Test
    public void ensureMainFailsIfNoHeap() throws Exception {
        final String schedulerImage = getTestConfig().getSchedulerImageName();
        CreateContainerCmd createCommand = CLUSTER.getConfig().dockerClient
                .createContainerCmd(schedulerImage)
                .withCmd(
                        ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "zk://" + "noIP" + ":2181/mesos"
                );

        CreateContainerResponse r = createCommand.exec();
        String containerId = r.getId();
        StartContainerCmd startMesosClusterContainerCmd = CLUSTER.getConfig().dockerClient.startContainerCmd(containerId);
        startMesosClusterContainerCmd.exec();
        String containerLog = containerLog(containerId);
        System.out.println(containerLog);
        assertTrue(containerLog.contains("Exception"));
        assertTrue(containerLog.contains("heap"));
    }

    @Test
    public void ensureMainFailsIfInvalidHeap() throws Exception {
        final String schedulerImage = getTestConfig().getSchedulerImageName();
        CreateContainerCmd createCommand = CLUSTER.getConfig().dockerClient
                .createContainerCmd(schedulerImage)
                .withEnv("JAVA_OPTS=-Xms128s1m -Xmx256f5m")
                .withCmd(
                        ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "zk://" + "noIP" + ":2181/mesos"
                );

        CreateContainerResponse r = createCommand.exec();
        String containerId = r.getId();
        StartContainerCmd startMesosClusterContainerCmd = CLUSTER.getConfig().dockerClient.startContainerCmd(containerId);
        startMesosClusterContainerCmd.exec();
        String containerLog = containerLog(containerId);
        System.out.println(containerLog);
        assertTrue(containerLog.contains("Invalid initial heap size"));
    }

    @Test
    public void ensureMainWorksIfValidHeap() throws Exception {
        final String schedulerImage = getTestConfig().getSchedulerImageName();
        CreateContainerCmd createCommand = CLUSTER.getConfig().dockerClient
                .createContainerCmd(schedulerImage)
                .withEnv("JAVA_OPTS=-Xms128m -Xmx256m")
                .withCmd(
                        ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL,
                        "zk://" + "noIP" + ":2181/mesos",
                        ElasticsearchCLIParameter.ELASTICSEARCH_NODES,
                        Integer.toString(getTestConfig().getElasticsearchNodesCount()),
                        Configuration.ELASTICSEARCH_RAM,
                        Integer.toString(getTestConfig().getElasticsearchMemorySize())
                );

        CreateContainerResponse r = createCommand.exec();
        String containerId = r.getId();
        StartContainerCmd startMesosClusterContainerCmd = CLUSTER.getConfig().dockerClient.startContainerCmd(containerId);
        startMesosClusterContainerCmd.exec();
        DockerUtil dockerUtil = new DockerUtil(CLUSTER.getConfig().dockerClient);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> !dockerUtil.getContainers().isEmpty());
        Boolean containerExists = dockerUtil.getContainers().stream().anyMatch(c -> c.getId().equals(containerId));
        assertTrue(containerExists);
    }

    private String containerLog(String containerId) throws Exception {
        return CLUSTER.getConfig().dockerClient.logContainerCmd(containerId).withStdOut().withStdErr().withFollowStream().exec(new LogContainerTestCallback()).awaitCompletion().toString();
    }
}
