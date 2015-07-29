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

    public String esPort = "9201";

    public DataPusherContainer(DockerClient dockerClient) {
        super(dockerClient);
    }

    @Override
    protected void pullImage() {
        dockerClient.pullImageCmd(pusherImageName);
    }

    @Override
    protected CreateContainerCmd dockerCommand() {
        return dockerClient.createContainerCmd(pusherImageName)
                .withName("es_pusher" + new SecureRandom().nextInt())
                .withEnv("ELASTICSEARCH_URL=" + "http://" + getIpAddress() + ":" + esPort)
                .withCmd("lein", "run", "-d");

    }
}
