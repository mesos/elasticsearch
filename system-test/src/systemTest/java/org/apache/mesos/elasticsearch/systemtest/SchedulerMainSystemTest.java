package org.apache.mesos.elasticsearch.systemtest;

import com.containersol.minimesos.MesosCluster;
import com.containersol.minimesos.mesos.MesosClusterConfig;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.command.StartContainerCmd;
import com.github.dockerjava.api.model.Container;
import com.jayway.awaitility.Awaitility;
import org.apache.mesos.elasticsearch.common.cli.ElasticsearchCLIParameter;
import org.apache.mesos.elasticsearch.common.cli.ZookeeperCLIParameter;
import org.apache.mesos.elasticsearch.scheduler.Configuration;
import org.junit.Test;

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
            .slaveResources(new String[]{"ports(*):[9200-9200,9300-9300]", "ports(*):[9201-9201,9301-9301]", "ports(*):[9202-9202,9302-9302]"})
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
        LogContainerTestCallback callback = new LogContainerTestCallback();
        CLUSTER.getConfig().dockerClient.logContainerCmd(containerId).withStdErr().exec(callback);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> !callback.toString().isEmpty());
        assertTrue(callback.toString().contains("Exception"));
        assertTrue(callback.toString().contains("heap"));
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
        LogContainerTestCallback callback = new LogContainerTestCallback();
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> !callback.toString().isEmpty());
        assertTrue(callback.toString().contains("Invalid initial heap size"));
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
