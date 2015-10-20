package org.apache.mesos.elasticsearch.systemtest;

import com.containersol.minimesos.container.AbstractContainer;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;

import java.security.SecureRandom;
import java.io.InputStream;

/**
 * Data Pusher container implementation
 */
public class DataPusherContainer extends AbstractContainer {

    public String pusherImageName = "alexglv/es-pusher";

    public String slaveAddress;

    public DataPusherContainer(DockerClient dockerClient, String firstSlaveHttpAddress) {
        super(dockerClient);
        slaveAddress = firstSlaveHttpAddress;
        this.pullImage();
    }

    @Override
    protected void pullImage() {
        pullImage(pusherImageName, "latest");
    }

    @Override
    protected CreateContainerCmd dockerCommand() {
        return dockerClient.createContainerCmd(pusherImageName)
                .withName("es_pusher" + new SecureRandom().nextInt())
                .withEnv("ELASTICSEARCH_URL=" + "http://" + slaveAddress);
//                .withCmd("lein", "run", "-d");
    }

    public InputStream getLogStreamStdOut() {
        return dockerClient.logContainerCmd(getContainerId()).withStdOut().withStdErr().exec(null);
    }
}
