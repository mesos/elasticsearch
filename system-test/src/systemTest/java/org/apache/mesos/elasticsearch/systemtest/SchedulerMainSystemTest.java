package org.apache.mesos.elasticsearch.systemtest;

import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.command.StartContainerCmd;
import com.github.dockerjava.api.model.Container;
import com.jayway.awaitility.Awaitility;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
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
    private static final Logger LOGGER = Logger.getLogger(SchedulerMainSystemTest.class);
    
    protected static final MesosCluster CLUSTER = new MesosCluster(
        MesosClusterConfig.builder()
            .numberOfSlaves(3)
            .privateRegistryPort(15000) // Currently you have to choose an available port by yourself
            .slaveResources(new String[]{"ports(*):[9200-9200,9300-9300]", "ports(*):[9201-9201,9301-9301]", "ports(*):[9202-9202,9302-9302]"})
            .build()
    );

    @Test
    public void ensureMainFailsIfNoHeap() throws Exception {
        final String schedulerImage = "mesos/elasticsearch-scheduler";
        CreateContainerCmd createCommand = CLUSTER.getConfig().dockerClient
                .createContainerCmd(schedulerImage)
                .withCmd(ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "zk://" + "noIP" + ":2181/mesos", ElasticsearchCLIParameter.ELASTICSEARCH_NODES, "3", Configuration.ELASTICSEARCH_RAM, "256");

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
        final String schedulerImage = "mesos/elasticsearch-scheduler";
        CreateContainerCmd createCommand = CLUSTER.getConfig().dockerClient
                .createContainerCmd(schedulerImage)
                .withEnv("JAVA_OPTS=-Xms128s1m -Xmx256f5m")
                .withCmd(ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "zk://" + "noIP" + ":2181/mesos", ElasticsearchCLIParameter.ELASTICSEARCH_NODES, "3", Configuration.ELASTICSEARCH_RAM, "256");

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
        String containerId = createSchedulerWithFrameworkName("elasticsearch");
        StartContainerCmd startMesosClusterContainerCmd = CLUSTER.getConfig().dockerClient.startContainerCmd(containerId);
        startMesosClusterContainerCmd.exec();
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> {
            List<Container> containers = CLUSTER.getConfig().dockerClient.listContainersCmd().exec();
            return containers.stream().anyMatch(c -> c.getId().equals(containerId));
        });
        List<Container> containers = CLUSTER.getConfig().dockerClient.listContainersCmd().exec();
        Boolean containerExists = containers.stream().anyMatch(c -> c.getId().equals(containerId));
        assertTrue(containerExists);
    }

    @Test
    public void ensureMainWorksIfStartingTwoSchedulers() throws Exception {
        String framework1ContainerId = createSchedulerWithFrameworkName("framework1");
        String framework2ContainerId = createSchedulerWithFrameworkName("framework2");

        CLUSTER.getConfig().dockerClient.startContainerCmd(framework1ContainerId).exec();
        CLUSTER.getConfig().dockerClient.startContainerCmd(framework2ContainerId).exec();

        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> {
            List<Container> containers = CLUSTER.getConfig().dockerClient.listContainersCmd().exec();
            return containers.stream().anyMatch(c -> c.getId().equals(framework1ContainerId)) &&
                   containers.stream().anyMatch(c -> c.getId().equals(framework2ContainerId));
        });
        List<Container> containers = CLUSTER.getConfig().dockerClient.listContainersCmd().exec();

        assertTrue(
                containers.stream().anyMatch(c -> c.getId().equals(framework1ContainerId)) &&
                containers.stream().anyMatch(c -> c.getId().equals(framework2ContainerId))
        );
    }

    private String createSchedulerWithFrameworkName(String frameworkName) {
        return CLUSTER
                .getConfig()
                .dockerClient
                .createContainerCmd("mesos/elasticsearch-scheduler")
                .withEnv("JAVA_OPTS=-Xms128m -Xmx256m")
                .withCmd(
                        ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "zk://" + "noIP" + ":2181/mesos",
                        ElasticsearchCLIParameter.ELASTICSEARCH_NODES, "3",
                        Configuration.ELASTICSEARCH_RAM, "256",
                        Configuration.FRAMEWORK_NAME, frameworkName
                )
                .exec()
                .getId();
    }
}
