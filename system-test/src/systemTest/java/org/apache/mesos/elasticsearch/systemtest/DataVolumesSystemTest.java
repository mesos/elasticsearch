package org.apache.mesos.elasticsearch.systemtest;

import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.apache.mesos.elasticsearch.scheduler.Configuration;
import org.apache.mesos.mini.MesosCluster;
import org.apache.mesos.mini.mesos.MesosClusterConfig;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * Tests data volumes
 */
@SuppressWarnings({"PMD.AvoidUsingHardCodedIP"})
public class DataVolumesSystemTest {

    public static final Logger LOGGER = Logger.getLogger(DataVolumesSystemTest.class);

    @Rule
    public final MesosCluster cluster = new MesosCluster(
        MesosClusterConfig.builder()
            .numberOfSlaves(3)
            .privateRegistryPort(15000) // Currently you have to choose an available port by yourself
            .slaveResources(new String[]{"ports(*):[9200-9200,9300-9300]", "ports(*):[9201-9201,9301-9301]", "ports(*):[9202-9202,9302-9302]"})
            .build()
    );

    @Before
    public void beforeScheduler() throws Exception {
        cluster.injectImage("mesos/elasticsearch-executor");
    }

    @After
    public void after() {
        cluster.stop();
    }

    @Test
    public void testDataVolumes() {
        LOGGER.info("Starting Elasticsearch scheduler");
        ElasticsearchSchedulerContainer scheduler = new ElasticsearchSchedulerContainer(cluster.getConfig().dockerClient, cluster.getMesosContainer().getIpAddress());
        cluster.addAndStartContainer(scheduler);
        LOGGER.info("Started Elasticsearch scheduler on " + scheduler.getIpAddress() + ":8080");

        TasksResponse tasksResponse = new TasksResponse(scheduler.getIpAddress(), cluster.getConfig().getNumberOfSlaves());

        List<JSONObject> tasks = tasksResponse.getTasks();

        ElasticsearchNodesResponse nodesResponse = new ElasticsearchNodesResponse(tasks, cluster.getConfig().getNumberOfSlaves());
        assertTrue("Elasticsearch nodes did not discover each other within 5 minutes", nodesResponse.isDiscoverySuccessful());

        ExecCreateCmdResponse execResponse = cluster.getConfig().dockerClient.execCreateCmd(cluster.getMesosContainer().getContainerId())
                .withCmd("ls", "-R", Configuration.DEFAULT_HOST_DATA_DIR)
                .withTty(true)
                .withAttachStderr()
                .withAttachStdout()
                .exec();
        try (InputStream inputstream = cluster.getConfig().dockerClient.execStartCmd(cluster.getMesosContainer().getContainerId()).withTty().withExecId(execResponse.getId()).exec()) {
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
    public void testDataVolumes_differentDataDir() {
        LOGGER.info("Starting Elasticsearch scheduler");
        ElasticsearchSchedulerContainer scheduler = new ElasticsearchSchedulerContainer(cluster.getConfig().dockerClient, cluster.getMesosContainer().getIpAddress());
        String dataDirectory = "/var/lib/mesos/slave";
        scheduler.setDataDirectory(dataDirectory);
        cluster.addAndStartContainer(scheduler);
        LOGGER.info("Started Elasticsearch scheduler on " + scheduler.getIpAddress() + ":8080");

        TasksResponse tasksResponse = new TasksResponse(scheduler.getIpAddress(), cluster.getConfig().getNumberOfSlaves());

        List<JSONObject> tasks = tasksResponse.getTasks();

        ElasticsearchNodesResponse nodesResponse = new ElasticsearchNodesResponse(tasks, cluster.getConfig().getNumberOfSlaves());
        assertTrue("Elasticsearch nodes did not discover each other within 5 minutes", nodesResponse.isDiscoverySuccessful());

        ExecCreateCmdResponse execResponse = cluster.getConfig().dockerClient.execCreateCmd(cluster.getMesosContainer().getContainerId())
                .withCmd("ls", "-R", dataDirectory)
                .withTty(true)
                .withAttachStderr()
                .withAttachStdout()
                .exec();
        try (InputStream inputstream = cluster.getConfig().dockerClient.execStartCmd(cluster.getMesosContainer().getContainerId()).withTty().withExecId(execResponse.getId()).exec()) {
            String contents = IOUtils.toString(inputstream);
            LOGGER.info("Mesos-local contents of " + dataDirectory);
            assertTrue(contents.contains("0"));
            assertTrue(contents.contains("1"));
            assertTrue(contents.contains("2"));
        } catch (IOException e) {
            LOGGER.error("Could not list contents of " + dataDirectory + " in Mesos-Local");
        }
    }

}
