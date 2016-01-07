package org.apache.mesos.elasticsearch.systemtest;

import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.jayway.awaitility.Awaitility;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.apache.mesos.elasticsearch.scheduler.Configuration;
import org.apache.mesos.elasticsearch.systemtest.base.SchedulerTestBase;
import org.apache.mesos.elasticsearch.systemtest.containers.AlpineContainer;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Tests data volumes
 */
public class DataVolumesSystemTest extends SchedulerTestBase {
    public static final Logger LOGGER = Logger.getLogger(DataVolumesSystemTest.class);

    @Test
    public void testDataVolumes() throws IOException {
        // Start a data container
        // When running on a mac, it is difficult to do an ls on the docker-machine VM. So instead, we mount a folder into another container and check the container.
        AlpineContainer dataContainer = new AlpineContainer(clusterArchitecture.dockerClient, Configuration.DEFAULT_HOST_DATA_DIR, Configuration.DEFAULT_HOST_DATA_DIR, new String[]{"sleep", "9999"});
        CLUSTER.addAndStartContainer(dataContainer);

        Awaitility.await().atMost(2L, TimeUnit.MINUTES).pollInterval(2L, TimeUnit.SECONDS).until(new DataInDirectory(dataContainer.getContainerId(), Configuration.DEFAULT_HOST_DATA_DIR));
    }

    @Test
    public void testDataVolumes_differentDataDir() throws IOException {
        String dataDirectory = "/var/lib/mesos/slave"; // TODO (pnw): How does this work? We're not passing it to the ES container?

        // Start a data container
        // When running on a mac, it is difficult to do an ls on the docker-machine VM. So instead, we mount a folder into another container and check the container.
        AlpineContainer dataContainer = new AlpineContainer(clusterArchitecture.dockerClient, Configuration.DEFAULT_HOST_DATA_DIR, Configuration.DEFAULT_HOST_DATA_DIR, new String[]{"sleep", "9999"});
        CLUSTER.addAndStartContainer(dataContainer);

        Awaitility.await().atMost(2L, TimeUnit.MINUTES).pollInterval(2L, TimeUnit.SECONDS).until(new DataInDirectory(dataContainer.getContainerId(), dataDirectory));
    }

    private static class DataInDirectory implements Callable<Boolean> {

        private final String containerId;
        private final String dataDirectory;

        private DataInDirectory(String containerId, String dataDirectory) {
            this.containerId = containerId;
            this.dataDirectory = dataDirectory;
        }

        @Override
        public Boolean call() throws Exception {
            ExecCreateCmdResponse execResponse = clusterArchitecture.dockerClient.execCreateCmd(containerId)
                    .withCmd("ls", "-R", dataDirectory)
                    .withTty(true)
                    .withAttachStderr()
                    .withAttachStdout()
                    .exec();
            try (InputStream inputstream = clusterArchitecture.dockerClient.execStartCmd(containerId).withTty().withExecId(execResponse.getId()).exec()) {
                String contents = IOUtils.toString(inputstream, "UTF-8");
                LOGGER.info("Mesos-local contents of " + dataDirectory);
                return contents.contains("0") && contents.contains("1") && contents.contains("2");
            } catch (IOException e) {
                LOGGER.error("Could not list contents of " + dataDirectory + " in Mesos-Local");
                return false;
            }
        }
    }
}
