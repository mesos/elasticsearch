package org.apache.mesos.elasticsearch.systemtest;

import com.containersol.minimesos.MesosCluster;
import com.containersol.minimesos.mesos.MesosClusterConfig;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Main app to run Mesos Elasticsearch with Mini Mesos.
 */
@SuppressWarnings({"PMD.AvoidUsingHardCodedIP"})
public class Main {

    public static final Logger LOGGER = Logger.getLogger(Main.class);

    public static void main(String[] args) throws InterruptedException {
        MesosCluster cluster = new MesosCluster(
            MesosClusterConfig.builder()
                .slaveResources(new String[]{"ports(*):[9200-9200,9300-9300]", "ports(*):[9201-9201,9301-9301]", "ports(*):[9202-9202,9302-9302]"})
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

        LOGGER.info("Starting scheduler");

        ElasticsearchSchedulerContainer scheduler = new ElasticsearchSchedulerContainer(cluster.getConfig().dockerClient, cluster.getZkContainer().getIpAddress());
        schedulerReference.set(scheduler);
        scheduler.start();

        seedData(cluster, scheduler);

        LOGGER.info("Scheduler started at http://" + scheduler.getIpAddress() + ":31100");
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
