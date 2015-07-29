package org.apache.mesos.elasticsearch.performancetest;

import com.github.dockerjava.api.DockerClient;
import org.apache.log4j.Logger;
import org.apache.mesos.mini.MesosCluster;
import org.apache.mesos.mini.mesos.MesosClusterConfig;

/**
 * Main app to run Mesos Elasticsearch with Mini Mesos.
 */
@SuppressWarnings({"PMD.AvoidUsingHardCodedIP"})
public class Main {

    public static final Logger LOGGER = Logger.getLogger(Main.class);

    private static DockerClient docker;

    public static final String MESOS_PORT = "5050";

    private static String schedulerId;
    private static String esPusherId;

    public static void main(String[] args) throws InterruptedException {
        MesosClusterConfig config = MesosClusterConfig.builder()
                .numberOfSlaves(3)
                .privateRegistryPort(15000) // Currently you have to choose an available port by yourself
                .slaveResources(new String[]{"ports(*):[9200-9200,9300-9300]", "ports(*):[9201-9201,9301-9301]", "ports(*):[9202-9202,9302-9302]"})
                .build();

        MesosCluster cluster = new MesosCluster(config);
        cluster.start();
        cluster.injectImage("mesos/elasticsearch-executor");

        LOGGER.info("Starting scheduler");

        ElasticsearchSchedulerContainer scheduler = new ElasticsearchSchedulerContainer(config.dockerClient, cluster.getMesosContainer().getIpAddress());

        DataPusherContainer pusher = new DataPusherContainer(config.dockerClient);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                scheduler.remove();
                cluster.stop();
            }
        });

        scheduler.start();
        pusher.start();
        LOGGER.info("Scheduler started at http://" + scheduler.getIpAddress() + ":8080");

        LOGGER.info("Type CTRL-C to quit");
        while (true) {
            Thread.sleep(1000);
        }
    }

    private static void shutdown() {
        docker.removeContainerCmd(schedulerId).withForce().exec();
        docker.removeContainerCmd(esPusherId).withForce().exec();
    }

}