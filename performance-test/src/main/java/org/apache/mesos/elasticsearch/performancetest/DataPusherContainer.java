package org.apache.mesos.elasticsearch.performancetest;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.sun.org.apache.bcel.internal.generic.PUSH;
import org.apache.mesos.mini.container.AbstractContainer;

import java.security.SecureRandom;

public class DataPusherContainer extends AbstractContainer {

    public String pusherImageName = "alexglv/es-pusher";

    public DataPusherContainer(DockerClient dockerClient) {
        super(dockerClient);
    }

    @Override
    protected void pullImage() {
        dockerUtil.pullImage(pusherImageName, "uberjar");
    }

    @Override
    protected CreateContainerCmd dockerCommand() {
        return dockerClient.createContainerCmd(pusherImageName)
                .withName("es_pusher" + new SecureRandom().nextInt())
                .withCmd("lein", "run", "-e", "http://" + getIpAddress() + ":9200", "-d");

    }
}
