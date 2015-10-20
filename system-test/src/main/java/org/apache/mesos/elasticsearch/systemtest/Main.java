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
                .numberOfSlaves(3)
                    .zkUrl("mesos")
                .slaveResources(new String[]{
                        "cpus(*):1;mem(*):1024;ports(*):[9200-9200,9300-9300]",
                        "cpus(*):1;mem(*):1024;ports(*):[9200-9200,9300-9300]",
                        "cpus(*):1;mem(*):1024;ports(*):[9200-9200,9300-9300]"})
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

        ElasticsearchSchedulerContainer scheduler = new ElasticsearchSchedulerContainer(cluster.getMesosMasterContainer().getOuterDockerClient(), cluster);
        LOGGER.info("Zookeeper path: " + cluster.getZkUrl());
        scheduler.setZookeeperFrameworkUrl(cluster.getZkUrl());
        schedulerReference.set(scheduler);
        scheduler.start();

        LOGGER.info("Scheduler started at http://" + scheduler.getIpAddress() + ":31100");
        LOGGER.info("Type CTRL-C to quit");
        while (true) {
            Thread.sleep(1000);
        }
    }

}
