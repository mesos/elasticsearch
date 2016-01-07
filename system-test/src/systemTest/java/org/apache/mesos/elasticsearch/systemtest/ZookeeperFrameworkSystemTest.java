package org.apache.mesos.elasticsearch.systemtest;

import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.log4j.Logger;
import org.apache.mesos.elasticsearch.systemtest.base.TestBase;
import org.apache.mesos.elasticsearch.systemtest.callbacks.ElasticsearchNodesResponse;
import org.apache.mesos.elasticsearch.systemtest.callbacks.ElasticsearchZookeeperResponse;
import org.apache.mesos.elasticsearch.systemtest.containers.ZookeeperContainer;
import org.apache.mesos.elasticsearch.systemtest.util.ContainerLifecycleManagement;
import org.apache.mesos.elasticsearch.systemtest.util.DockerUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * System tests which verifies configuring a separate Zookeeper CLUSTER for the framework.
 */
@SuppressWarnings({"PMD.AvoidUsingHardCodedIP"})
public class ZookeeperFrameworkSystemTest extends TestBase {
    private static final Logger LOGGER = Logger.getLogger(ZookeeperFrameworkSystemTest.class);
    private ZookeeperContainer zookeeper;
    private ElasticsearchSchedulerContainer scheduler;
    private static final ContainerLifecycleManagement CONTAINER_LIFECYCLE_MANAGEMENT = new ContainerLifecycleManagement();

    @Before
    public void before() {
        LOGGER.info("Starting Extra zookeeper container");
        zookeeper = new ZookeeperContainer(clusterArchitecture.dockerClient);
        CLUSTER.addAndStartContainer(zookeeper, TEST_CONFIG.getClusterTimeout());
        scheduler = new ElasticsearchSchedulerContainer(clusterArchitecture.dockerClient, CLUSTER.getZkContainer().getIpAddress(), CLUSTER);
    }

    @After
    public void after() {
        CONTAINER_LIFECYCLE_MANAGEMENT.stopAll();
        new DockerUtil(clusterArchitecture.dockerClient).killAllExecutors();
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
        CONTAINER_LIFECYCLE_MANAGEMENT.addAndStart(scheduler, TEST_CONFIG.getClusterTimeout());
        ESTasks esTasks = new ESTasks(TEST_CONFIG, scheduler.getIpAddress(), true);

        new TasksResponse(esTasks, TEST_CONFIG.getElasticsearchNodesCount());

        ElasticsearchNodesResponse nodesResponse = new ElasticsearchNodesResponse(esTasks, TEST_CONFIG.getElasticsearchNodesCount());
        assertTrue("Elasticsearch nodes did not discover each other within 5 minutes", nodesResponse.isDiscoverySuccessful());

        ElasticsearchZookeeperResponse elasticsearchZookeeperResponse = new ElasticsearchZookeeperResponse(new ESTasks(TEST_CONFIG, scheduler.getIpAddress(), true));
        assertEquals("zk://" + zookeeper.getIpAddress() + ":2181", elasticsearchZookeeperResponse.getHost());
    }

    @Test
    public void testZookeeperFramework_differentPath() throws UnirestException {
        scheduler.setZookeeperFrameworkUrl("zk://" + zookeeper.getIpAddress() + ":2181/framework");
        CONTAINER_LIFECYCLE_MANAGEMENT.addAndStart(scheduler, TEST_CONFIG.getClusterTimeout());
        ESTasks esTasks = new ESTasks(TEST_CONFIG, scheduler.getIpAddress(), true);

        new TasksResponse(esTasks, TEST_CONFIG.getElasticsearchNodesCount());

        ElasticsearchNodesResponse nodesResponse = new ElasticsearchNodesResponse(esTasks, TEST_CONFIG.getElasticsearchNodesCount());
        assertTrue("Elasticsearch nodes did not discover each other within 5 minutes", nodesResponse.isDiscoverySuccessful());

        ElasticsearchZookeeperResponse elasticsearchZookeeperResponse = new ElasticsearchZookeeperResponse(new ESTasks(TEST_CONFIG, scheduler.getIpAddress(), true));
        assertEquals("zk://" + zookeeper.getIpAddress() + ":2181", elasticsearchZookeeperResponse.getHost());
    }
}
