package org.apache.mesos.elasticsearch.systemtest;

import com.containersol.minimesos.cluster.MesosCluster;
import com.containersol.minimesos.docker.DockerClientFactory;
import com.containersol.minimesos.mesos.MesosClusterContainersFactory;
import com.github.dockerjava.api.DockerClient;
import org.apache.log4j.Logger;
import org.apache.mesos.elasticsearch.systemtest.containers.ElasticsearchSchedulerContainer;
import org.apache.mesos.elasticsearch.systemtest.util.DockerUtil;
import org.apache.mesos.elasticsearch.systemtest.util.IpTables;

import java.util.Scanner;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Main app to run Mesos Elasticsearch on minimesos.
 */
public class Main implements Runnable {

    public static final Logger LOGGER = Logger.getLogger(Main.class);
    public static final Configuration TEST_CONFIG = new Configuration();

    private static DockerClient dockerClient;

    private volatile boolean keepRunning = true;

    @Override
    public void run() {
        dockerClient = DockerClientFactory.build();

        MesosClusterContainersFactory factory = new MesosClusterContainersFactory();

        MesosCluster mesosCluster = factory.createMesosCluster("src/main/resources/minimesosFile");
        mesosCluster.setMapPortsToHost(true);

        final AtomicReference<ElasticsearchSchedulerContainer> schedulerReference = new AtomicReference<>(null);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                if (schedulerReference.get() != null) {
                    schedulerReference.get().remove();
                }

                mesosCluster.destroy(factory);
                new DockerUtil(dockerClient).killAllExecutors();
            }
        });
        mesosCluster.start(30);
        IpTables.apply(dockerClient, mesosCluster, TEST_CONFIG);

        LOGGER.info("Starting scheduler");
        ElasticsearchSchedulerContainer scheduler = new ElasticsearchSchedulerContainer(dockerClient, mesosCluster.getZooKeeper().getIpAddress());
        schedulerReference.set(scheduler);
        mesosCluster.addAndStartProcess(scheduler, TEST_CONFIG.getClusterTimeout());

        seedData(mesosCluster, scheduler);

        LOGGER.info("Scheduler started at http://" + scheduler.getIpAddress() + ":" + TEST_CONFIG.getSchedulerGuiPort());
        LOGGER.info("Type 'q' to quit");

        while (keepRunning) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                LOGGER.info("Shutting down...");
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        Main main = new Main();
        Thread thread = new Thread(main);
        thread.start();

        Scanner s = new Scanner(System.in);
        while (!s.next().equals("q"));

        main.keepRunning = false;
        thread.interrupt();
    }

    private static void seedData(MesosCluster cluster, ElasticsearchSchedulerContainer schedulerContainer) {
        String taskHttpAddress;
        try {
            ESTasks esTasks = new ESTasks(TEST_CONFIG, schedulerContainer.getIpAddress());
            new TasksResponse(esTasks, TEST_CONFIG.getElasticsearchNodesCount(), "TASK_RUNNING");
            taskHttpAddress = esTasks.getEsHttpAddressList().get(0);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
        SeedDataContainer seedData = new SeedDataContainer(dockerClient, "http://" + taskHttpAddress);
        cluster.addAndStartProcess(seedData, TEST_CONFIG.getClusterTimeout());
        LOGGER.info("Elasticsearch node " + taskHttpAddress + " seeded with data");
    }
}
