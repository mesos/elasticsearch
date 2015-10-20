package org.apache.mesos.elasticsearch.systemtest;

import org.apache.log4j.Logger;
import org.apache.mesos.mini.MesosCluster;
import org.apache.mesos.mini.mesos.MesosClusterConfig;
import org.json.JSONObject;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Main app to run Mesos Elasticsearch with Mini Mesos.
 */
public class Main {

    public static final Logger LOGGER = Logger.getLogger(Main.class);
    public static final Configuration TEST_CONFIG = new Configuration();

    public static void main(String[] args) throws InterruptedException {
        MesosCluster cluster = new MesosCluster(
            MesosClusterConfig.builder()
                .numberOfSlaves(TEST_CONFIG.getElasticsearchNodesCount())
                .privateRegistryPort(TEST_CONFIG.getPrivateRegistryPort()) // Currently you have to choose an available port by yourself
                .slaveResources(TEST_CONFIG.getPortRanges())
                .build()
        );

        final AtomicReference<ElasticsearchSchedulerContainer> schedulerReference = new AtomicReference<>(null);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                if (schedulerReference.get() != null) {
                    schedulerReference.get().remove();
                }
                cluster.stop();
            }
        });
        cluster.start();
        cluster.injectImage(TEST_CONFIG.getExecutorImageName());

        LOGGER.info("Starting scheduler");

        ElasticsearchSchedulerContainer scheduler = new ElasticsearchSchedulerContainer(cluster.getConfig().dockerClient, cluster.getMesosContainer().getIpAddress());
        schedulerReference.set(scheduler);
        scheduler.start();

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
            List<JSONObject> tasks = new TasksResponse(schedulerContainer.getIpAddress(), cluster.getConfig().getNumberOfSlaves(), "TASK_RUNNING").getTasks();
            taskHttpAddress = tasks.get(0).getString("http_address");
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
        SeedDataContainer seedData = new SeedDataContainer(cluster.getConfig().dockerClient, "http://" + taskHttpAddress);
        cluster.addAndStartContainer(seedData);
        LOGGER.info("Elasticsearch node " + taskHttpAddress + " seeded with data");
    }

}
