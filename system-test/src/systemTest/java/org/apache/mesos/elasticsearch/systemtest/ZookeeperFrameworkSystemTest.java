package org.apache.mesos.elasticsearch.systemtest;

import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.log4j.Logger;
import org.apache.mesos.elasticsearch.systemtest.base.TestBase;
import org.apache.mesos.elasticsearch.systemtest.callbacks.ElasticsearchNodesResponse;
import org.apache.mesos.elasticsearch.systemtest.callbacks.ElasticsearchZookeeperResponse;
import org.apache.mesos.elasticsearch.systemtest.containers.ZookeeperContainer;
import org.apache.mesos.elasticsearch.systemtest.util.ContainerLifecycleManagement;
import org.json.JSONObject;
import org.junit.*;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * System tests which verifies configuring a separate Zookeeper CLUSTER for the framework.
 */
@SuppressWarnings({"PMD.AvoidUsingHardCodedIP"})
public class ZookeeperFrameworkSystemTest extends TestBase {
    private static final Logger LOGGER = Logger.getLogger(ZookeeperFrameworkSystemTest.class);
    private static ZookeeperContainer zookeeper;
    private ElasticsearchSchedulerContainer scheduler;
    private static final ContainerLifecycleManagement CONTAINER_LIFECYCLE_MANAGEMENT = new ContainerLifecycleManagement();

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
        CONTAINER_LIFECYCLE_MANAGEMENT.stopAll();
    }

    @ClassRule
    public static final TestWatcher WATCHER = new TestWatcher() {
        @Override
        protected void failed(Throwable e, Description description) {
            CONTAINER_LIFECYCLE_MANAGEMENT.stopAll();
        }
    };

    @Test
    public void testZookeeperFramework() throws UnirestException {
        scheduler.setZookeeperFrameworkUrl("zk://" + zookeeper.getIpAddress() + ":2181");
        CONTAINER_LIFECYCLE_MANAGEMENT.addAndStart(scheduler);

        TasksResponse tasksResponse = new TasksResponse(scheduler.getIpAddress(), CLUSTER.getConfig().getNumberOfSlaves());

        List<JSONObject> tasks = tasksResponse.getTasks();

        ElasticsearchNodesResponse nodesResponse = new ElasticsearchNodesResponse(tasks, CLUSTER.getConfig().getNumberOfSlaves());
        assertTrue("Elasticsearch nodes did not discover each other within 5 minutes", nodesResponse.isDiscoverySuccessful());

        ElasticsearchZookeeperResponse elasticsearchZookeeperResponse = new ElasticsearchZookeeperResponse(tasks.get(0).getString("http_address"));
        assertEquals("zk://" + zookeeper.getIpAddress() + ":2181", elasticsearchZookeeperResponse.getHost());
    }

    @Test
    public void testZookeeperFramework_differentPath() throws UnirestException {
        scheduler.setZookeeperFrameworkUrl("zk://" + zookeeper.getIpAddress() + ":2181/framework");
        CONTAINER_LIFECYCLE_MANAGEMENT.addAndStart(scheduler);

        TasksResponse tasksResponse = new TasksResponse(scheduler.getIpAddress(), CLUSTER.getConfig().getNumberOfSlaves());

        List<JSONObject> tasks = tasksResponse.getTasks();

        ElasticsearchNodesResponse nodesResponse = new ElasticsearchNodesResponse(tasks, CLUSTER.getConfig().getNumberOfSlaves());
        assertTrue("Elasticsearch nodes did not discover each other within 5 minutes", nodesResponse.isDiscoverySuccessful());

        ElasticsearchZookeeperResponse elasticsearchZookeeperResponse = new ElasticsearchZookeeperResponse(tasks.get(0).getString("http_address"));
        assertEquals("zk://" + zookeeper.getIpAddress() + ":2181", elasticsearchZookeeperResponse.getHost());
    }
}
