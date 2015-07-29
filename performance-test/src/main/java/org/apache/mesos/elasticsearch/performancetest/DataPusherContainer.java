package org.apache.mesos.elasticsearch.performancetest;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import org.apache.mesos.mini.container.AbstractContainer;

import java.security.SecureRandom;

/**
 * Data Pusher container implementation
 */
public class DataPusherContainer extends AbstractContainer {

    public String pusherImageName = "alexglv/es-pusher";

    public String slaveAddress;

    public DataPusherContainer(DockerClient dockerClient, String firstSlaveHttpAddress) {
        super(dockerClient);
        slaveAddress = firstSlaveHttpAddress;
    }

    @Override
    protected void pullImage() {
        dockerClient.pullImageCmd(pusherImageName);
    }

    @Override
    protected CreateContainerCmd dockerCommand() {
        return dockerClient.createContainerCmd(pusherImageName)
                .withName("es_pusher" + new SecureRandom().nextInt())
                .withEnv("ELASTICSEARCH_URL=" + "http://" + slaveAddress)
                .withCmd("lein", "run", "-d");

    }
}
