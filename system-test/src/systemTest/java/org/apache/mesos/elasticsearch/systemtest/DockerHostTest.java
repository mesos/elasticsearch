package org.apache.mesos.elasticsearch.systemtest;

import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.command.StartContainerCmd;
import com.github.dockerjava.api.model.Container;
import com.jayway.awaitility.Awaitility;
import org.apache.commons.io.IOUtils;
import org.apache.mesos.mini.mesos.MesosClusterConfig;
import org.junit.Test;

import java.io.InputStream;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

/**
 * Tests code that ensures docker host mode.
 */
public class DockerHostTest {
    private static final MesosClusterConfig CONFIG = MesosClusterConfig.builder()
            .numberOfSlaves(3)
            .privateRegistryPort(15000) // Currently you have to choose an available port by yourself
            .slaveResources(new String[]{"ports(*):[9200-9200,9300-9300]", "ports(*):[9201-9201,9301-9301]", "ports(*):[9202-9202,9302-9302]"})
            .build();

    @Test
    public void ensureMainFailsIfNotInHostMode() throws Exception {
        final String schedulerImage = "mesos/elasticsearch-scheduler";
        CreateContainerCmd createCommand = CONFIG.dockerClient
                .createContainerCmd(schedulerImage)
                .withEnv("JAVA_OPTS=-Xms128m -Xmx256m")
                .withCmd("-zk", "zk://" + "noIP" + ":2181/mesos", "-n", "3", "-ram", "64");

        CreateContainerResponse r = createCommand.exec();
        String containerId = r.getId();
        StartContainerCmd startMesosClusterContainerCmd = CONFIG.dockerClient.startContainerCmd(containerId);
        startMesosClusterContainerCmd.exec();
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> {
            InputStream exec = CONFIG.dockerClient.logContainerCmd(containerId).withStdErr().exec();
            return !IOUtils.toString(exec).isEmpty();
        });
        InputStream exec = CONFIG.dockerClient.logContainerCmd(containerId).withStdErr().exec();
        String log = IOUtils.toString(exec);
        assertTrue(log.contains("Exception"));
        assertTrue(log.contains("HOST"));
    }

    @Test
    public void ensureMainWorksIfValidHost() throws Exception {
        final String schedulerImage = "mesos/elasticsearch-scheduler";
        CreateContainerCmd createCommand = CONFIG.dockerClient
                .createContainerCmd(schedulerImage)
                .withNetworkMode("host")
                .withEnv("JAVA_OPTS=-Xms128m -Xmx256m")
                .withCmd("-zk", "zk://" + "noIP" + ":2181/mesos", "-n", "3", "-ram", "64");

        CreateContainerResponse r = createCommand.exec();
        String containerId = r.getId();
        StartContainerCmd startMesosClusterContainerCmd = CONFIG.dockerClient.startContainerCmd(containerId);
        startMesosClusterContainerCmd.exec();
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> {
            List<Container> containers = CONFIG.dockerClient.listContainersCmd().exec();
            return !containers.isEmpty();
        });
        List<Container> containers = CONFIG.dockerClient.listContainersCmd().exec();
        Boolean containerExists = containers.stream().anyMatch(c -> c.getId().equals(containerId));
        assertTrue(containerExists);
    }
}
