package org.apache.mesos.elasticsearch.systemtest;

import com.containersol.minimesos.container.AbstractContainer;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.Volume;
import com.jayway.awaitility.Awaitility;
import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.apache.mesos.elasticsearch.scheduler.Configuration;
import org.apache.mesos.elasticsearch.systemtest.base.SchedulerTestBase;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.security.SecureRandom;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

/**
 * Tests data volumes
 */
public class DataVolumesSystemTest extends SchedulerTestBase {
    public static final Logger LOGGER = Logger.getLogger(DataVolumesSystemTest.class);

    @Test
    public void testDataVolumes() throws IOException {
        // Start a data container
        // When running on a mac, it is difficult to do an ls on the docker-machine VM. So instead, we mount a folder into another container and check the container.
        AlpineContainer dataContainer = new AlpineContainer(CLUSTER.getConfig().dockerClient, Configuration.DEFAULT_HOST_DATA_DIR, Configuration.DEFAULT_HOST_DATA_DIR);
        CLUSTER.addAndStartContainer(dataContainer);

        Awaitility.await().atMost(2L, TimeUnit.MINUTES).pollInterval(2L, TimeUnit.SECONDS).until(new DataInDirectory(dataContainer.getContainerId(), Configuration.DEFAULT_HOST_DATA_DIR));
    }

    @Test
    public void testDataVolumes_differentDataDir() throws IOException {
        String dataDirectory = "/var/lib/mesos/slave";

        // Start a data container
        // When running on a mac, it is difficult to do an ls on the docker-machine VM. So instead, we mount a folder into another container and check the container.
        AlpineContainer dataContainer = new AlpineContainer(CLUSTER.getConfig().dockerClient, Configuration.DEFAULT_HOST_DATA_DIR, Configuration.DEFAULT_HOST_DATA_DIR);
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
            ExecCreateCmdResponse execResponse = CLUSTER.getConfig().dockerClient.execCreateCmd(containerId)
                    .withCmd("ls", "-R", dataDirectory)
                    .withTty(true)
                    .withAttachStderr()
                    .withAttachStdout()
                    .exec();
            try (InputStream inputstream = CLUSTER.getConfig().dockerClient.execStartCmd(containerId).withTty().withExecId(execResponse.getId()).exec()) {
                String contents = IOUtils.toString(inputstream, "UTF-8");
                LOGGER.info("Mesos-local contents of " + dataDirectory);
                return contents.contains("0") && contents.contains("1") && contents.contains("2");
            } catch (IOException e) {
                LOGGER.error("Could not list contents of " + dataDirectory + " in Mesos-Local");
                return false;
            }
        }
    }

    /**
     * A wrapper for an alpine data container
     */
    public static class AlpineContainer extends AbstractContainer {

        public static final String ALPINE_IMAGE_NAME = "alpine";
        private final String hostVolume;
        private final String containerVolume;

        public AlpineContainer(DockerClient dockerClient, String hostVolume, String containerVolume) {
            super(dockerClient);
            this.hostVolume = hostVolume;
            this.containerVolume = containerVolume;
        }

        @Override
        protected void pullImage() {
            pullImage(ALPINE_IMAGE_NAME, "latest");
        }

        @Override
        protected CreateContainerCmd dockerCommand() {
            return dockerClient
                    .createContainerCmd(ALPINE_IMAGE_NAME)
                    .withName("Alpine" + "_" + new SecureRandom().nextInt())
                    .withBinds(new Bind(hostVolume, new Volume(containerVolume)))
                    .withCmd("sleep", "9999");
        }
    }
}
