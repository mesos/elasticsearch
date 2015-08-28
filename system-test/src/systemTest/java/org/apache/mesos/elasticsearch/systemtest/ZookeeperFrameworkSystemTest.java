package org.apache.mesos.elasticsearch.systemtest;

import com.mashape.unirest.http.exceptions.UnirestException;
import org.json.JSONObject;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * System tests which verifies configuring a separate Zookeeper cluster for the framework.
 */
public class ZookeeperFrameworkSystemTest extends TestBase {

    @Test
    public void testZookeeperFramework() throws UnirestException {
        ZookeeperContainer zookeeper = new ZookeeperContainer(CONFIG.dockerClient);

        CLUSTER.addAndStartContainer(zookeeper);

        ElasticsearchSchedulerContainer scheduler = getScheduler();
        scheduler.setZookeeperFrameworkUrl("http://" + zookeeper.getIpAddress() + ":2181");

        TasksResponse tasksResponse = new TasksResponse(scheduler.getIpAddress(), NODE_COUNT);

        List<JSONObject> tasks = tasksResponse.getTasks();

        ElasticsearchNodesResponse nodesResponse = new ElasticsearchNodesResponse(tasks, NODE_COUNT);
        assertTrue("Elasticsearch nodes did not discover each other within 5 minutes", nodesResponse.isDiscoverySuccessful());

        ElasticsearchZookeeperResponse elasticsearchZookeeperResponse = new ElasticsearchZookeeperResponse(tasks.get(0).getString("http_address"));
        assertEquals("http://" + zookeeper.getIpAddress() + ":2181", elasticsearchZookeeperResponse.getHost());
    }

}
