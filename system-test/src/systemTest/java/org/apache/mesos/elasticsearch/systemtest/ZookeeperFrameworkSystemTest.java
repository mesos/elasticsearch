package org.apache.mesos.elasticsearch.systemtest;

import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.log4j.Logger;
import org.apache.mesos.mini.MesosCluster;
import org.apache.mesos.mini.mesos.MesosClusterConfig;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * System tests which verifies configuring a separate Zookeeper cluster for the framework.
 */
@SuppressWarnings({"PMD.AvoidUsingHardCodedIP"})
public class ZookeeperFrameworkSystemTest {

    protected static final int NODE_COUNT = 3;

    protected static final MesosClusterConfig CONFIG = MesosClusterConfig.builder()
            .numberOfSlaves(NODE_COUNT)
            .privateRegistryPort(15000) // Currently you have to choose an available port by yourself
            .slaveResources(new String[]{"ports(*):[9200-9200,9300-9300]", "ports(*):[9201-9201,9301-9301]", "ports(*):[9202-9202,9302-9302]"})
            .build();

    private static final Logger LOGGER = Logger.getLogger(TestBase.class);

    @Rule
    public final MesosCluster CLUSTER = new MesosCluster(CONFIG);

    private ElasticsearchSchedulerContainer scheduler;

    private ZookeeperContainer zookeeper;

    @Rule
    public TestWatcher watchman = new TestWatcher() {
        @Override
        protected void failed(Throwable e, Description description) {
            CLUSTER.stop();
            scheduler.remove();
        }
    };

    @Before
    public void startScheduler() throws Exception {
        CLUSTER.injectImage("mesos/elasticsearch-executor");

        LOGGER.info("Starting Elasticsearch scheduler");

        zookeeper = new ZookeeperContainer(CONFIG.dockerClient);
        CLUSTER.addAndStartContainer(zookeeper);

        scheduler = new ElasticsearchSchedulerContainer(CONFIG.dockerClient, CLUSTER.getMesosContainer().getIpAddress());

        LOGGER.info("Started Elasticsearch scheduler on " + scheduler.getIpAddress() + ":8080");
    }

    @Test
    public void testZookeeperFramework() throws UnirestException {
        scheduler.setZookeeperFrameworkUrl("zk://" + zookeeper.getIpAddress() + ":2181");
        CLUSTER.addAndStartContainer(scheduler);

        TasksResponse tasksResponse = new TasksResponse(scheduler.getIpAddress(), NODE_COUNT);

        List<JSONObject> tasks = tasksResponse.getTasks();

        ElasticsearchNodesResponse nodesResponse = new ElasticsearchNodesResponse(tasks, NODE_COUNT);
        assertTrue("Elasticsearch nodes did not discover each other within 5 minutes", nodesResponse.isDiscoverySuccessful());

        ElasticsearchZookeeperResponse elasticsearchZookeeperResponse = new ElasticsearchZookeeperResponse(tasks.get(0).getString("http_address"));
        assertEquals("zk://" + zookeeper.getIpAddress() + ":2181", elasticsearchZookeeperResponse.getHost());
    }

    @Test
    public void testZookeeperFramework_differentPath() throws UnirestException {
        scheduler.setZookeeperFrameworkUrl("zk://" + zookeeper.getIpAddress() + ":2181/framework");
        CLUSTER.addAndStartContainer(scheduler);

        TasksResponse tasksResponse = new TasksResponse(scheduler.getIpAddress(), NODE_COUNT);

        List<JSONObject> tasks = tasksResponse.getTasks();

        ElasticsearchNodesResponse nodesResponse = new ElasticsearchNodesResponse(tasks, NODE_COUNT);
        assertTrue("Elasticsearch nodes did not discover each other within 5 minutes", nodesResponse.isDiscoverySuccessful());

        ElasticsearchZookeeperResponse elasticsearchZookeeperResponse = new ElasticsearchZookeeperResponse(tasks.get(0).getString("http_address"));
        assertEquals("zk://" + zookeeper.getIpAddress() + ":2181", elasticsearchZookeeperResponse.getHost());
    }

}
