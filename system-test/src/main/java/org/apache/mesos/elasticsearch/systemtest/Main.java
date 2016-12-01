package org.apache.mesos.elasticsearch.systemtest;

import com.containersol.minimesos.cluster.MesosCluster;
import com.containersol.minimesos.docker.DockerClientFactory;
import com.containersol.minimesos.mesos.MesosClusterContainersFactory;
import com.github.dockerjava.api.DockerClient;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.apache.mesos.elasticsearch.systemtest.containers.ElasticsearchSchedulerContainer;
import org.apache.mesos.elasticsearch.systemtest.util.DockerUtil;
import org.apache.mesos.elasticsearch.systemtest.util.IpTables;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Main app to run Mesos Elasticsearch on minimesos.
 */
public class Main implements Runnable {

    public static final Logger LOGGER = Logger.getLogger(Main.class);

    public final Configuration TEST_CONFIG = new Configuration();

    private DockerClient dockerClient;

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

        try (InputStreamReader reader = new InputStreamReader(new FileInputStream("src/main/resources/elasticsearch.json"), "UTF-8")) {
            mesosCluster.getMarathon().deployApp(IOUtils.toString(reader));
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error("Could not deploy Mesos Elasticsearch via Marathon");
        }

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
        main.runMainThread();
    }

    @SuppressWarnings("PMD.EmptyWhileStmt")
    private void runMainThread() throws InterruptedException {
        Main framework = new Main();
        Thread frameworkThread = new Thread(framework);
        frameworkThread.start();

        Scanner s = new Scanner(System.in, "UTF-8");
        while (!s.next().equals("q")) {
            Thread.sleep(1000);
        }

        framework.keepRunning = false;
        frameworkThread.interrupt();
    }
}
