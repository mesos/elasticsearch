package org.apache.mesos.elasticsearch.systemtest;

import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.command.StartContainerCmd;
import com.jayway.awaitility.Awaitility;
import org.apache.commons.io.IOUtils;
import org.apache.mesos.mini.mesos.MesosClusterConfig;
import org.junit.Test;

import java.io.InputStream;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

/**
 * Tests the main method.
 */
public class SchedulerMainTest {
    protected static final MesosClusterConfig config = MesosClusterConfig.builder()
            .numberOfSlaves(3)
            .privateRegistryPort(15000) // Currently you have to choose an available port by yourself
            .slaveResources(new String[]{"ports(*):[9200-9200,9300-9300]", "ports(*):[9201-9201,9301-9301]", "ports(*):[9202-9202,9302-9302]"})
            .build();

    @Test
    public void ensureMainFailsIfNoHeap() throws Exception {
        final String schedulerImage = "mesos/elasticsearch-scheduler";
        CreateContainerCmd createCommand = config.dockerClient
                .createContainerCmd(schedulerImage)
                .withCmd("-zk", "zk://" + "noIP" + ":2181/mesos", "-n", "3", "-ram", "64");

        CreateContainerResponse r = createCommand.exec();
        String containerId = r.getId();
        StartContainerCmd startMesosClusterContainerCmd = config.dockerClient.startContainerCmd(containerId);
        startMesosClusterContainerCmd.exec();
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> {
            InputStream exec = config.dockerClient.logContainerCmd(containerId).withStdErr().exec();
            return !IOUtils.toString(exec).isEmpty();
        });
        InputStream exec = config.dockerClient.logContainerCmd(containerId).withStdErr().exec();
        String log = IOUtils.toString(exec);
        assertTrue(log.contains("Exception"));
        assertTrue(log.contains("heap"));
    }
}
