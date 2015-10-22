package org.apache.mesos.elasticsearch.systemtest;

import com.containersol.minimesos.container.AbstractContainer;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.Volume;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.apache.mesos.elasticsearch.scheduler.Configuration;
import org.apache.mesos.elasticsearch.systemtest.base.TestBase;
import org.apache.mesos.elasticsearch.systemtest.callbacks.ElasticsearchNodesResponse;
import org.json.JSONObject;
import org.junit.*;

import java.io.IOException;
import java.io.InputStream;
import java.security.SecureRandom;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * Tests data volumes
 */
public class DataVolumesSystemTest extends TestBase {
    public static final Logger LOGGER = Logger.getLogger(DataVolumesSystemTest.class);

    @Test
    public void testDataVolumes() throws IOException {
        LOGGER.info("Starting Elasticsearch scheduler");
        ElasticsearchSchedulerContainer scheduler = new ElasticsearchSchedulerContainer(CLUSTER.getConfig().dockerClient, CLUSTER.getZkContainer().getIpAddress());
        CLUSTER.addAndStartContainer(scheduler);
        LOGGER.info("Started Elasticsearch scheduler on " + scheduler.getIpAddress() + ":" + getTestConfig().getSchedulerGuiPort());

        TasksResponse tasksResponse = new TasksResponse(scheduler.getIpAddress(), CLUSTER.getConfig().getNumberOfSlaves());

        List<JSONObject> tasks = tasksResponse.getTasks();

        ElasticsearchNodesResponse nodesResponse = new ElasticsearchNodesResponse(tasks, CLUSTER.getConfig().getNumberOfSlaves());
        assertTrue("Elasticsearch nodes did not discover each other within 5 minutes", nodesResponse.isDiscoverySuccessful());

        // Start a data container
        // When running on a mac, it is difficult to do an ls on the docker-machine VM. So instead, we mount a folder into another container and check the container.
        AlpineContainer dataContainer = new AlpineContainer(CLUSTER.getConfig().dockerClient, Configuration.DEFAULT_HOST_DATA_DIR, Configuration.DEFAULT_HOST_DATA_DIR);
        CLUSTER.addAndStartContainer(dataContainer);

        ExecCreateCmdResponse execResponse = CLUSTER.getConfig().dockerClient.execCreateCmd(dataContainer.getContainerId())
                .withCmd("ls", "-R", Configuration.DEFAULT_HOST_DATA_DIR)
                .withTty(true)
                .withAttachStderr()
                .withAttachStdout()
                .exec();
        try (InputStream inputstream = CLUSTER.getConfig().dockerClient.execStartCmd(dataContainer.getContainerId()).withTty().withExecId(execResponse.getId()).exec()) {
            String contents = IOUtils.toString(inputstream);
            LOGGER.info("Mesos-local contents of " + Configuration.DEFAULT_HOST_DATA_DIR + "/elasticsearch/nodes: " + contents);
            assertTrue(contents.contains("0"));
            assertTrue(contents.contains("1"));
            assertTrue(contents.contains("2"));
        } catch (IOException e) {
            LOGGER.error("Could not list contents of " + Configuration.DEFAULT_HOST_DATA_DIR + "/elasticsearch/nodes in Mesos-Local");
        }
    }

    @Test
    public void testDataVolumes_differentDataDir() throws IOException {
        LOGGER.info("Starting Elasticsearch scheduler");
        ElasticsearchSchedulerContainer scheduler = new ElasticsearchSchedulerContainer(CLUSTER.getConfig().dockerClient, CLUSTER.getZkContainer().getIpAddress());
        String dataDirectory = "/var/lib/mesos/slave";
        scheduler.setDataDirectory(dataDirectory);
        CLUSTER.addAndStartContainer(scheduler);
        LOGGER.info("Started Elasticsearch scheduler on " + scheduler.getIpAddress() + ":" + getTestConfig().getSchedulerGuiPort());

        TasksResponse tasksResponse = new TasksResponse(scheduler.getIpAddress(), CLUSTER.getConfig().getNumberOfSlaves());

        List<JSONObject> tasks = tasksResponse.getTasks();

        ElasticsearchNodesResponse nodesResponse = new ElasticsearchNodesResponse(tasks, CLUSTER.getConfig().getNumberOfSlaves());
        assertTrue("Elasticsearch nodes did not discover each other within 5 minutes", nodesResponse.isDiscoverySuccessful());

        // Start a data container
        // When running on a mac, it is difficult to do an ls on the docker-machine VM. So instead, we mount a folder into another container and check the container.
        AlpineContainer dataContainer = new AlpineContainer(CLUSTER.getConfig().dockerClient, Configuration.DEFAULT_HOST_DATA_DIR, Configuration.DEFAULT_HOST_DATA_DIR);
        CLUSTER.addAndStartContainer(dataContainer);

        ExecCreateCmdResponse execResponse = CLUSTER.getConfig().dockerClient.execCreateCmd(dataContainer.getContainerId())
                .withCmd("ls", "-R", dataDirectory)
                .withTty(true)
                .withAttachStderr()
                .withAttachStdout()
                .exec();
        try (InputStream inputstream = CLUSTER.getConfig().dockerClient.execStartCmd(dataContainer.getContainerId()).withTty().withExecId(execResponse.getId()).exec()) {
            String contents = IOUtils.toString(inputstream);
            LOGGER.info("Mesos-local contents of " + dataDirectory);
            assertTrue(contents.contains("0"));
            assertTrue(contents.contains("1"));
            assertTrue(contents.contains("2"));
        } catch (IOException e) {
            LOGGER.error("Could not list contents of " + dataDirectory + " in Mesos-Local");
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
