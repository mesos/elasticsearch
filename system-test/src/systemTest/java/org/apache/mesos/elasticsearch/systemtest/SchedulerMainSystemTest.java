package org.apache.mesos.elasticsearch.systemtest;

import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.command.StartContainerCmd;
import com.github.dockerjava.api.model.Container;
import com.jayway.awaitility.Awaitility;
import org.apache.commons.io.IOUtils;
import org.apache.mesos.elasticsearch.common.cli.ElasticsearchCLIParameter;
import org.apache.mesos.elasticsearch.common.cli.ZookeeperCLIParameter;
import org.apache.mesos.elasticsearch.scheduler.Configuration;
import org.apache.mesos.mini.MesosCluster;
import org.apache.mesos.mini.mesos.MesosClusterConfig;
import org.junit.Test;

import java.io.InputStream;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

/**
 * Tests the main method.
 */
public class SchedulerMainSystemTest {

    protected static final org.apache.mesos.elasticsearch.systemtest.Configuration TEST_CONFIG = new org.apache.mesos.elasticsearch.systemtest.Configuration();
    
    protected static final MesosCluster CLUSTER = new MesosCluster(
        MesosClusterConfig.builder()
            .numberOfSlaves(TEST_CONFIG.getElasticsearchNodesCount())
            .privateRegistryPort(TEST_CONFIG.getPrivateRegistryPort()) // Currently you have to choose an available port by yourself
            .slaveResources(TEST_CONFIG.getPortRanges())
            .build()
    );

    @Test
    public void ensureMainFailsIfNoHeap() throws Exception {
        final String schedulerImage = TEST_CONFIG.getSchedulerImageName();
        CreateContainerCmd createCommand = CLUSTER.getConfig().dockerClient
                .createContainerCmd(schedulerImage)
                .withCmd(
                    ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL,
                    "zk://" + "noIP" + ":2181/mesos",
                    ElasticsearchCLIParameter.ELASTICSEARCH_NODES,
                    Integer.toString(TEST_CONFIG.getElasticsearchNodesCount()),
                    Configuration.ELASTICSEARCH_RAM,
                    Integer.toString(TEST_CONFIG.getElasticsearchMemorySize())
                );

        CreateContainerResponse r = createCommand.exec();
        String containerId = r.getId();
        StartContainerCmd startMesosClusterContainerCmd = CLUSTER.getConfig().dockerClient.startContainerCmd(containerId);
        startMesosClusterContainerCmd.exec();
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> {
            InputStream exec = CLUSTER.getConfig().dockerClient.logContainerCmd(containerId).withStdErr().exec();
            return !IOUtils.toString(exec).isEmpty();
        });
        InputStream exec = CLUSTER.getConfig().dockerClient.logContainerCmd(containerId).withStdErr().exec();
        String log = IOUtils.toString(exec);
        assertTrue(log.contains("Exception"));
        assertTrue(log.contains("heap"));
    }

    @Test
    public void ensureMainFailsIfInvalidHeap() throws Exception {
        final String schedulerImage = TEST_CONFIG.getSchedulerImageName();
        CreateContainerCmd createCommand = CLUSTER.getConfig().dockerClient
                .createContainerCmd(schedulerImage)
                .withEnv("JAVA_OPTS=-Xms128s1m -Xmx256f5m")
                .withCmd(
                    ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL,
                    "zk://" + "noIP" + ":2181/mesos",
                    ElasticsearchCLIParameter.ELASTICSEARCH_NODES,
                    Integer.toString(TEST_CONFIG.getElasticsearchNodesCount()),
                    Configuration.ELASTICSEARCH_RAM,
                    Integer.toString(TEST_CONFIG.getElasticsearchMemorySize())
                );

        CreateContainerResponse r = createCommand.exec();
        String containerId = r.getId();
        StartContainerCmd startMesosClusterContainerCmd = CLUSTER.getConfig().dockerClient.startContainerCmd(containerId);
        startMesosClusterContainerCmd.exec();
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> {
            InputStream exec = CLUSTER.getConfig().dockerClient.logContainerCmd(containerId).withStdErr().exec();
            return !IOUtils.toString(exec).isEmpty();
        });
        InputStream exec = CLUSTER.getConfig().dockerClient.logContainerCmd(containerId).withStdErr().exec();
        String log = IOUtils.toString(exec);
        assertTrue(log.contains("Invalid initial heap size"));
    }



    @Test
    public void ensureMainWorksIfValidHeap() throws Exception {
        final String schedulerImage = TEST_CONFIG.getSchedulerImageName();
        CreateContainerCmd createCommand = CLUSTER.getConfig().dockerClient
                .createContainerCmd(schedulerImage)
                .withEnv("JAVA_OPTS=-Xms128m -Xmx256m")
                .withCmd(
                    ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL,
                    "zk://" + "noIP" + ":2181/mesos",
                    ElasticsearchCLIParameter.ELASTICSEARCH_NODES,
                    Integer.toString(TEST_CONFIG.getElasticsearchNodesCount()),
                    Configuration.ELASTICSEARCH_RAM,
                    Integer.toString(TEST_CONFIG.getElasticsearchMemorySize())
                );

        CreateContainerResponse r = createCommand.exec();
        String containerId = r.getId();
        StartContainerCmd startMesosClusterContainerCmd = CLUSTER.getConfig().dockerClient.startContainerCmd(containerId);
        startMesosClusterContainerCmd.exec();
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> {
            List<Container> containers = CLUSTER.getConfig().dockerClient.listContainersCmd().exec();
            return !containers.isEmpty();
        });
        List<Container> containers = CLUSTER.getConfig().dockerClient.listContainersCmd().exec();
        Boolean containerExists = containers.stream().anyMatch(c -> c.getId().equals(containerId));
        assertTrue(containerExists);
    }
}
