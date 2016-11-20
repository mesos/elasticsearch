package org.apache.mesos.elasticsearch.systemtest;

import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.jayway.awaitility.Awaitility;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.apache.mesos.elasticsearch.scheduler.Configuration;
import org.apache.mesos.elasticsearch.systemtest.base.TestBase;
import org.apache.mesos.elasticsearch.systemtest.callbacks.ElasticsearchNodesResponse;
import org.apache.mesos.elasticsearch.systemtest.containers.AlpineContainer;
import org.apache.mesos.elasticsearch.systemtest.containers.ElasticsearchSchedulerContainer;
import org.apache.mesos.elasticsearch.systemtest.util.DockerUtil;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

/**
 * Tests data volumes
 */
@Ignore("This test has to be merged into DeploymentSystemTest. See https://github.com/mesos/elasticsearch/issues/591")
public class DataVolumesSystemTest extends TestBase {
    public static final Logger LOGGER = Logger.getLogger(DataVolumesSystemTest.class);

    @Test
    public void testDataVolumes() throws IOException {
        ElasticsearchSchedulerContainer schedulerContainer = startScheduler(Configuration.DEFAULT_HOST_DATA_DIR);
        // Start a data container
        // When running on a mac, it is difficult to do an ls on the docker-machine VM. So instead, we mount a folder into another container and check the container.
        AlpineContainer dataContainer = new AlpineContainer(getDockerClient(), Configuration.DEFAULT_HOST_DATA_DIR, Configuration.DEFAULT_HOST_DATA_DIR, "sleep", "9999");
        CLUSTER.addAndStartProcess(dataContainer, TEST_CONFIG.getClusterTimeout());

        Awaitility.await().atMost(1L, TimeUnit.MINUTES).pollInterval(2L, TimeUnit.SECONDS).until(new DataInDirectory(dataContainer.getContainerId(), Configuration.DEFAULT_HOST_DATA_DIR));
        stopScheduler(schedulerContainer);
    }

    @Test
    public void testDataVolumes_differentDataDir() throws IOException {
        String dataDirectory = "/var/lib/mesos/test";
        ElasticsearchSchedulerContainer schedulerContainer = startScheduler(dataDirectory);

        // Start a data container
        // When running on a mac, it is difficult to do an ls on the docker-machine VM. So instead, we mount a folder into another container and check the container.
        AlpineContainer dataContainer = new AlpineContainer(getDockerClient(), dataDirectory, dataDirectory, "sleep", "9999");
        CLUSTER.addAndStartProcess(dataContainer, TEST_CONFIG.getClusterTimeout());

        Awaitility.await().atMost(1L, TimeUnit.MINUTES).pollInterval(2L, TimeUnit.SECONDS).until(new DataInDirectory(dataContainer.getContainerId(), dataDirectory));
        stopScheduler(schedulerContainer);
    }

    public ElasticsearchSchedulerContainer startScheduler(String dataDir) {
        LOGGER.info("Starting Elasticsearch scheduler");

        ElasticsearchSchedulerContainer scheduler = new ElasticsearchSchedulerContainer(getDockerClient(), CLUSTER.getZooKeeper().getIpAddress(), dataDir);
        CLUSTER.addAndStartProcess(scheduler, TEST_CONFIG.getClusterTimeout());

        LOGGER.info("Started Elasticsearch scheduler on " + scheduler.getIpAddress() + ":" + getTestConfig().getSchedulerGuiPort());

        ESTasks esTasks = new ESTasks(TEST_CONFIG, scheduler.getIpAddress());
        new TasksResponse(esTasks, TEST_CONFIG.getElasticsearchNodesCount());

        ElasticsearchNodesResponse nodesResponse = new ElasticsearchNodesResponse(esTasks, TEST_CONFIG.getElasticsearchNodesCount());
        assertTrue("Elasticsearch nodes did not discover each other within 5 minutes", nodesResponse.isDiscoverySuccessful());

        return scheduler;
    }

    private void stopScheduler(ElasticsearchSchedulerContainer schedulerContainer) {
        schedulerContainer.remove();
        CLUSTER.getMemberProcesses().remove(schedulerContainer);
        new DockerUtil(getDockerClient()).killAllExecutors();
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
            ExecCreateCmdResponse execResponse = getDockerClient().execCreateCmd(containerId)
                    .withCmd("ls", "-R", dataDirectory)
                    .withTty(true)
                    .withAttachStdout(true)
                    .withAttachStderr(true)
                    .exec();
            try (InputStream inputstream = getDockerClient().execStartCmd(containerId).withTty(true).withExecId(execResponse.getId()).exec(null)) {
                String contents = IOUtils.toString(inputstream, Charset.defaultCharset()).replaceAll("\\p{C}", "");
                LOGGER.info("Contents of " + dataDirectory + ": " + contents);
                return contents.contains("S0") && contents.contains("S1") && contents.contains("S2");
            } catch (IOException e) {
                LOGGER.error("Could not list contents of " + dataDirectory + " in Mesos-Local");
                return false;
            }
        }
    }
}
