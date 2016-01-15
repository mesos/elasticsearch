package org.apache.mesos.elasticsearch.systemtest;

import com.containersol.minimesos.MesosCluster;
import com.containersol.minimesos.mesos.ClusterArchitecture;
import org.apache.log4j.Logger;
import org.apache.mesos.elasticsearch.systemtest.util.DockerUtil;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Main app to run Mesos Elasticsearch with Mini Mesos.
 */
public class Main {

    public static final Logger LOGGER = Logger.getLogger(Main.class);
    public static final Configuration TEST_CONFIG = new Configuration();

    private static ClusterArchitecture clusterArchitecture;

    public static void main(String[] args) throws InterruptedException {
        clusterArchitecture = new ClusterArchitecture.Builder()
                .withZooKeeper()
                .withMaster()
                .withSlave(TEST_CONFIG.getPortRanges()[0])
                .withSlave(TEST_CONFIG.getPortRanges()[1])
                .withSlave(TEST_CONFIG.getPortRanges()[2])
                .build();
        MesosCluster cluster = new MesosCluster(clusterArchitecture);

        final AtomicReference<ElasticsearchSchedulerContainer> schedulerReference = new AtomicReference<>(null);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                if (schedulerReference.get() != null) {
                    schedulerReference.get().remove();
                }

                cluster.stop();
                new DockerUtil(clusterArchitecture.dockerClient).killAllExecutors();
            }
        });
        cluster.start(TEST_CONFIG.getClusterTimeout());

        LOGGER.info("Starting scheduler");
        ElasticsearchSchedulerContainer scheduler = new ElasticsearchSchedulerContainer(clusterArchitecture.dockerClient, cluster.getZkContainer().getIpAddress(), cluster);
        schedulerReference.set(scheduler);
        cluster.addAndStartContainer(scheduler, TEST_CONFIG.getClusterTimeout());

        seedData(cluster, scheduler);

        LOGGER.info("Scheduler started at http://" + scheduler.getIpAddress() + ":" + TEST_CONFIG.getSchedulerGuiPort());
        LOGGER.info("Type CTRL-C to quit");
        while (true) {
            Thread.sleep(1000);
        }
    }

    private static void seedData(MesosCluster cluster, ElasticsearchSchedulerContainer schedulerContainer) {
        String taskHttpAddress;
        try {
            ESTasks esTasks = new ESTasks(TEST_CONFIG, schedulerContainer.getIpAddress(), true);
            new TasksResponse(esTasks, TEST_CONFIG.getElasticsearchNodesCount(), "TASK_RUNNING");
            taskHttpAddress = esTasks.getEsHttpAddressList().get(0);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
        SeedDataContainer seedData = new SeedDataContainer(clusterArchitecture.dockerClient, "http://" + taskHttpAddress);
        cluster.addAndStartContainer(seedData, TEST_CONFIG.getClusterTimeout());
        LOGGER.info("Elasticsearch node " + taskHttpAddress + " seeded with data");
    }

}
