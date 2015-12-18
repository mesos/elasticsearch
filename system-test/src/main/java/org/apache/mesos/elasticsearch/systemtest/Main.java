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
                .withSlave("ports:[30000-31000]")
                .withSlave("ports:[31001-32000]")
                .withSlave("ports:[32001-33000]")
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
        cluster.start();

        LOGGER.info("Starting scheduler");
        ElasticsearchSchedulerContainer scheduler = new ElasticsearchSchedulerContainer(clusterArchitecture.dockerClient, cluster.getZkContainer().getIpAddress(), cluster);
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
            ESTasks esTasks = new ESTasks(TEST_CONFIG, schedulerContainer.getIpAddress());
            new TasksResponse(esTasks, 3, "TASK_RUNNING");
            taskHttpAddress = esTasks.getTasks().get(0).getString("http_address");
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
        SeedDataContainer seedData = new SeedDataContainer(clusterArchitecture.dockerClient, "http://" + taskHttpAddress);
        cluster.addAndStartContainer(seedData);
        LOGGER.info("Elasticsearch node " + taskHttpAddress + " seeded with data");
    }

}
