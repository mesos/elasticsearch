package org.apache.mesos.elasticsearch.systemtest;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * Tests data volumes
 */
public class DataVolumesSystemTest extends TestBase {

    public static final Logger LOGGER = Logger.getLogger(DiscoverySystemTest.class);

    @Test
    public void testDataVolumes() {
        ElasticsearchSchedulerContainer scheduler = getScheduler();

        TasksResponse tasksResponse = new TasksResponse(scheduler.getIpAddress(), NODE_COUNT);

        List<JSONObject> tasks = tasksResponse.getTasks();

        ElasticsearchNodesResponse nodesResponse = new ElasticsearchNodesResponse(tasks, NODE_COUNT);
        assertTrue("Elasticsearch nodes did not discover each other within 5 minutes", nodesResponse.isDiscoverySuccessful());

        try (InputStream inputstream = CONFIG.dockerClient.copyFileFromContainerCmd(CLUSTER.getMesosContainer().getContainerId(), "/var/lib/elasticsearch/mesos-elasticsearch").withHostPath("/tmp").exec()) {
            String contents = IOUtils.toString(inputstream);
            inputstream.close();
            assertTrue(contents.contains("mesos-elasticsearch/nodes"));
            assertTrue(contents.contains("mesos-elasticsearch/nodes/0"));
            assertTrue(contents.contains("mesos-elasticsearch/nodes/0/_state"));
            assertTrue(contents.contains("mesos-elasticsearch/nodes/0/node.lock"));
            assertTrue(contents.contains("mesos-elasticsearch/nodes/1"));
            assertTrue(contents.contains("mesos-elasticsearch/nodes/1/_state"));
            assertTrue(contents.contains("mesos-elasticsearch/nodes/1/node.lock"));
            assertTrue(contents.contains("mesos-elasticsearch/nodes/2"));
            assertTrue(contents.contains("mesos-elasticsearch/nodes/2/_state"));
            assertTrue(contents.contains("mesos-elasticsearch/nodes/2/node.lock"));
        } catch (IOException e) {
            LOGGER.error("Could not copy /var/lib/elasticsearch/mesos-elasticsearch from Mesos-Local container");
        }
    }

}
