package org.apache.mesos.elasticsearch.systemtest;

import com.containersol.minimesos.cluster.MesosCluster;
import com.containersol.minimesos.mesos.ClusterArchitecture;
import com.containersol.minimesos.mesos.DockerClientFactory;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.jayway.awaitility.Awaitility;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.mesos.elasticsearch.common.cli.ZookeeperCLIParameter;
import org.apache.mesos.elasticsearch.systemtest.callbacks.ElasticsearchNodesResponse;
import org.apache.mesos.elasticsearch.systemtest.containers.ElasticsearchSchedulerContainer;
import org.apache.mesos.elasticsearch.systemtest.containers.MesosMasterTagged;
import org.apache.mesos.elasticsearch.systemtest.containers.MesosSlaveTagged;
import org.apache.mesos.elasticsearch.systemtest.util.DockerUtil;
import org.apache.mesos.elasticsearch.systemtest.util.IpTables;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.mesos.elasticsearch.scheduler.Configuration.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests that we can run different versions of ES
 */
public class DifferentESVersionSystemTest {
    private static final Logger LOG = LoggerFactory.getLogger(DifferentESVersionSystemTest.class);
    public static final String ES_VERSION = "2.0.2";
    public static final String ES_IMAGE = "elasticsearch:" + ES_VERSION;
    public static final String ES_BINARY = "https://download.elasticsearch.org/elasticsearch/release/org/elasticsearch/distribution/tar/elasticsearch/" + ES_VERSION + "/elasticsearch-" + ES_VERSION + ".tar.gz";
    protected static final Configuration TEST_CONFIG = new Configuration();
    private final DockerClient dockerClient = DockerClientFactory.build();
    private final DockerUtil dockerUtil = new DockerUtil(dockerClient);
    private MesosCluster cluster;

    @Before
    public void before() {
        dockerUtil.killAllSchedulers();
        dockerUtil.killAllExecutors();
        final ClusterArchitecture clusterArchitecture = new ClusterArchitecture.Builder()
                .withZooKeeper()
                .withMaster(MesosMasterTagged::new)
                .withAgent(zooKeeper -> new MesosSlaveTagged(zooKeeper, TEST_CONFIG.getPortRanges().get(0)))
                .withAgent(zooKeeper -> new MesosSlaveTagged(zooKeeper, TEST_CONFIG.getPortRanges().get(1)))
                .withAgent(zooKeeper -> new MesosSlaveTagged(zooKeeper, TEST_CONFIG.getPortRanges().get(2)))
                .build();
        cluster = new MesosCluster(clusterArchitecture);
        cluster.setExposedHostPorts(true);
        cluster.start(TEST_CONFIG.getClusterTimeout());
    }

    @After
    public void after() {
        dockerUtil.killAllSchedulers();
        dockerUtil.killAllExecutors();
        if (cluster != null) {
            cluster.stop();
        }
    }


    @Test
    public void shouldStartDockerImage() {
        IpTables.apply(dockerClient, cluster, TEST_CONFIG); // Only forward docker traffic

        final DockerESVersionScheduler scheduler = new DockerESVersionScheduler(dockerClient, cluster.getZkContainer().getIpAddress(), cluster, ES_IMAGE);
        cluster.addAndStartContainer(scheduler, TEST_CONFIG.getClusterTimeout());
        LOG.info("Started Elasticsearch scheduler on " + scheduler.getIpAddress() + ":" + TEST_CONFIG.getSchedulerGuiPort());

        ESTasks esTasks = new ESTasks(TEST_CONFIG, scheduler.getIpAddress());
        new TasksResponse(esTasks, TEST_CONFIG.getElasticsearchNodesCount());

        ElasticsearchNodesResponse nodesResponse = new ElasticsearchNodesResponse(esTasks, TEST_CONFIG.getElasticsearchNodesCount());
        assertTrue("Elasticsearch nodes did not discover each other within 5 minutes", nodesResponse.isDiscoverySuccessful());

        final AtomicReference<String> versionNumber = new AtomicReference<>("");
        Awaitility.await().pollInterval(1L, TimeUnit.SECONDS).atMost(TEST_CONFIG.getClusterTimeout(), TimeUnit.SECONDS).until(() -> {
            try {
                versionNumber.set(Unirest.get("http://" + esTasks.getEsHttpAddressList().get(0)).asJson().getBody().getObject().getJSONObject("version").getString("number"));
                return true;
            } catch (UnirestException e) {
                return false;
            }
        });
        assertEquals("ES version is not the same as requested: " + ES_VERSION + " != " + versionNumber.get(), ES_VERSION, versionNumber.get());
    }

    @Test
    public void shouldStartJar() {
        // Don't forward traffic. Jars are actually running on the slaves
        final JarESVersionScheduler scheduler = new JarESVersionScheduler(dockerClient, cluster.getZkContainer().getIpAddress(), cluster, ES_BINARY);
        cluster.addAndStartContainer(scheduler, TEST_CONFIG.getClusterTimeout());
        LOG.info("Started Elasticsearch scheduler on " + scheduler.getIpAddress() + ":" + TEST_CONFIG.getSchedulerGuiPort());

        ESTasks esTasks = new ESTasks(TEST_CONFIG, scheduler.getIpAddress());
        new TasksResponse(esTasks, TEST_CONFIG.getElasticsearchNodesCount());

        ElasticsearchNodesResponse nodesResponse = new ElasticsearchNodesResponse(esTasks, TEST_CONFIG.getElasticsearchNodesCount());
        assertTrue("Elasticsearch nodes did not discover each other within 5 minutes", nodesResponse.isDiscoverySuccessful());

        final AtomicReference<String> versionNumber = new AtomicReference<>("");
        Awaitility.await().pollInterval(1L, TimeUnit.SECONDS).atMost(TEST_CONFIG.getClusterTimeout(), TimeUnit.SECONDS).until(() -> {
            try {
                versionNumber.set(Unirest.get("http://" + esTasks.getEsHttpAddressList().get(0)).asJson().getBody().getObject().getJSONObject("version").getString("number"));
                return true;
            } catch (UnirestException e) {
                return false;
            }
        });
        assertEquals("ES version is not the same as requested: " + ES_VERSION + " != " + versionNumber.get(), ES_VERSION, versionNumber.get());
    }

    /**
     * Versioned docker scheduler
     */
    private static class DockerESVersionScheduler extends ElasticsearchSchedulerContainer {

        private final String image;

        public DockerESVersionScheduler(DockerClient dockerClient, String zkIp, MesosCluster cluster, String image) {
            super(dockerClient, zkIp, cluster);
            this.image = image;
        }

        @Override
        public void pullImage() {
            super.pullImage();
            dockerClient.pullImageCmd(image);
        }

        @Override
        protected CreateContainerCmd dockerCommand() {
            return dockerClient
                    .createContainerCmd(TEST_CONFIG.getSchedulerImageName())
                    .withName(TEST_CONFIG.getSchedulerName() + "_" + new SecureRandom().nextInt())
                    .withEnv("JAVA_OPTS=-Xms128m -Xmx256m")
                    .withCmd(
                            ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, getZookeeperMesosUrl(),
                            ELASTICSEARCH_RAM, Integer.toString(TEST_CONFIG.getElasticsearchMemorySize()),
                            ELASTICSEARCH_CPU, "0.1",
                            ELASTICSEARCH_DISK, "150",
                            EXECUTOR_IMAGE, image,
                            FRAMEWORK_USE_DOCKER, "true");
        }
    }

    /**
     * Versioned Jar Scheduler
     */
    private static class JarESVersionScheduler extends ElasticsearchSchedulerContainer {

        private final String binaryUrl;

        public JarESVersionScheduler(DockerClient dockerClient, String zkIp, MesosCluster cluster, String binaryUrl) {
            super(dockerClient, zkIp, cluster);
            this.binaryUrl = binaryUrl;
        }

        @Override
        protected CreateContainerCmd dockerCommand() {
            return dockerClient
                    .createContainerCmd(TEST_CONFIG.getSchedulerImageName())
                    .withName(TEST_CONFIG.getSchedulerName() + "_" + new SecureRandom().nextInt())
                    .withEnv("JAVA_OPTS=-Xms128m -Xmx256m")
                    .withCmd(
                            ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, getZookeeperMesosUrl(),
                            ELASTICSEARCH_RAM, Integer.toString(TEST_CONFIG.getElasticsearchMemorySize()),
                            ELASTICSEARCH_CPU, "0.1",
                            ELASTICSEARCH_DISK, "150",
                            EXECUTOR_BINARY, binaryUrl,
                            FRAMEWORK_USE_DOCKER, "false");
        }
    }
}
