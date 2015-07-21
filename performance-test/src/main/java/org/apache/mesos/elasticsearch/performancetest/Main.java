package org.apache.mesos.elasticsearch.performancetest;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import org.apache.log4j.Logger;
import org.apache.mesos.mini.MesosCluster;
import org.apache.mesos.mini.docker.DockerUtil;
import org.apache.mesos.mini.mesos.MesosClusterConfig;
import org.apache.mesos.mini.container.AbstractContainer;
import org.apache.mesos.mini.docker.DockerProxy;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.command.InspectContainerResponse;
import java.util.stream.IntStream;

/**
 * Main app to run Mesos Elasticsearch with Mini Mesos.
 */
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

        docker = config.dockerClient;

        Runtime.getRuntime().addShutdownHook(new Thread(Main::shutdown));

        MesosCluster cluster = new MesosCluster(config);
        cluster.start();

        cluster.injectImage("mesos/elasticsearch-executor");

        String ipAddress = cluster.getMesosContainer().getMesosMasterURL().replace(":" + MESOS_PORT, "");

        final String schedulerImage = "mesos/elasticsearch-scheduler";

        CreateContainerCmd createCommand = docker
                .createContainerCmd(schedulerImage)
                .withExtraHosts(IntStream.rangeClosed(1, config.numberOfSlaves).mapToObj(value -> "slave" + value + ":" + ipAddress).toArray(String[]::new))
                .withCmd("-zk", "zk://" + ipAddress + ":2181/mesos", "-n", "3", "-m", "9999", "-ram", "64");

        DockerUtil dockerUtil = new DockerUtil(config.dockerClient);
        schedulerId = dockerUtil.createAndStart(createCommand);

        String containerId = cluster.getMesosContainer().getMesosContainerID();
        InspectContainerResponse response = docker.inspectContainerCmd(containerId).exec();
        String esIp = response.getNetworkSettings().getIpAddress();

        final String esImage = "alexglv/es-pusher";

        CreateContainerCmd createPusher = docker
                .createContainerCmd(esImage)
                .withCmd("-e", "http://"+ esIp + ":9200", "-d");
        esPusherId = dockerUtil.createAndStart(createPusher);

        DockerClient dockerClient = config.dockerClient;
        DockerProxy proxy = new DockerProxy(dockerClient);
        proxy.start();
        
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

class EsPusherContainer extends AbstractContainer {

    public static final String HELLO_WORLD_IMAGE = "alexglv/es-pusher";
    public static final int PORT = 80;

    protected EsPusherContainer(DockerClient dockerClient) {
        super(dockerClient);
    }

    @Override
    protected void pullImage() {
        dockerUtil.pullImage(HELLO_WORLD_IMAGE, "latest");
    }

    @Override
    protected CreateContainerCmd dockerCommand() {
        return dockerClient.createContainerCmd(HELLO_WORLD_IMAGE).withPortBindings(PortBinding.parse("0.0.0.0:" + PORT + ":" + PORT));
    }
}
