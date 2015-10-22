package org.apache.mesos.elasticsearch.systemtest;

import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.log4j.Logger;
import org.apache.mesos.elasticsearch.systemtest.base.SchedulerTestBase;
import org.apache.mesos.elasticsearch.systemtest.base.TestBase;
import org.apache.mesos.elasticsearch.systemtest.callbacks.ElasticsearchNodesResponse;
import org.apache.mesos.elasticsearch.systemtest.callbacks.ElasticsearchZookeeperResponse;
import org.apache.mesos.elasticsearch.systemtest.containers.ZookeeperContainer;
import org.apache.mesos.elasticsearch.systemtest.util.ContainerLifecycleManagement;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * System tests which verifies configuring a separate Zookeeper CLUSTER for the framework.
 */
public class ZookeeperFrameworkSystemTest extends TestBase {

    private static final Logger LOGGER = Logger.getLogger(ZookeeperFrameworkSystemTest.class);
    private static ZookeeperContainer zookeeper;
    private static ElasticsearchSchedulerContainer scheduler;
    private final ContainerLifecycleManagement containerManagement = new ContainerLifecycleManagement();

    public static ElasticsearchSchedulerContainer getScheduler() {
        return scheduler;
    }

    @BeforeClass
    public static void startZookeeper() throws Exception {
        LOGGER.info("Starting Extra zookeeper container");
        zookeeper = new ZookeeperContainer(CLUSTER.getConfig().dockerClient);
        CLUSTER.addAndStartContainer(zookeeper);
    }

    @Before
    public void before() {
        scheduler = new ElasticsearchSchedulerContainer(CLUSTER.getConfig().dockerClient, CLUSTER.getZkContainer().getIpAddress());
    }

    @After
    public void after() {
        containerManagement.stopAll();
    }

    @Test
    public void testZookeeperFramework() throws UnirestException {
        getScheduler().setZookeeperFrameworkUrl("zk://" + zookeeper.getIpAddress() + ":2181");
        containerManagement.addAndStart(scheduler);

        TasksResponse tasksResponse = new TasksResponse(scheduler.getIpAddress(), CLUSTER.getConfig().getNumberOfSlaves());

        List<JSONObject> tasks = tasksResponse.getTasks();

        ElasticsearchNodesResponse nodesResponse = new ElasticsearchNodesResponse(tasks, CLUSTER.getConfig().getNumberOfSlaves());
        assertTrue("Elasticsearch nodes did not discover each other within 5 minutes", nodesResponse.isDiscoverySuccessful());

        ElasticsearchZookeeperResponse elasticsearchZookeeperResponse = new ElasticsearchZookeeperResponse(tasks.get(0).getString("http_address"));
        assertEquals("zk://" + zookeeper.getIpAddress() + ":2181", elasticsearchZookeeperResponse.getHost());
    }

    @Test
    public void testZookeeperFramework_differentPath() throws UnirestException {
        getScheduler().setZookeeperFrameworkUrl("zk://" + zookeeper.getIpAddress() + ":2181/framework");
        containerManagement.addAndStart(scheduler);

        TasksResponse tasksResponse = new TasksResponse(getScheduler().getIpAddress(), CLUSTER.getConfig().getNumberOfSlaves());

        List<JSONObject> tasks = tasksResponse.getTasks();

        ElasticsearchNodesResponse nodesResponse = new ElasticsearchNodesResponse(tasks, CLUSTER.getConfig().getNumberOfSlaves());
        assertTrue("Elasticsearch nodes did not discover each other within 5 minutes", nodesResponse.isDiscoverySuccessful());

        ElasticsearchZookeeperResponse elasticsearchZookeeperResponse = new ElasticsearchZookeeperResponse(tasks.get(0).getString("http_address"));
        assertEquals("zk://" + zookeeper.getIpAddress() + ":2181", elasticsearchZookeeperResponse.getHost());
    }
}
