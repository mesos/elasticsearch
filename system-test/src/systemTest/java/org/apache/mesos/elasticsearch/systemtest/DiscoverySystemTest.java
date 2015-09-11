package org.apache.mesos.elasticsearch.systemtest;

import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * Tests REST node discovery
 */
public class DiscoverySystemTest extends TestBase {

    public static final Logger LOGGER = Logger.getLogger(DiscoverySystemTest.class);

    @Test
    public void testNodeDiscoveryRest() {
        ElasticsearchSchedulerContainer scheduler = getScheduler();

        TasksResponse tasksResponse = new TasksResponse(scheduler.getIpAddress(), CLUSTER.getConfig().getNumberOfSlaves());

        List<JSONObject> tasks = tasksResponse.getTasks();

        ElasticsearchNodesResponse nodesResponse = new ElasticsearchNodesResponse(tasks, CLUSTER.getConfig().getNumberOfSlaves());
        assertTrue("Elasticsearch nodes did not discover each other within 5 minutes", nodesResponse.isDiscoverySuccessful());
   }

}
