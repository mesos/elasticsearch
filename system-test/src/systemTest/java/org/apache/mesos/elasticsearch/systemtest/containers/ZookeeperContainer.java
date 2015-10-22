package org.apache.mesos.elasticsearch.systemtest.containers;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.containersol.minimesos.container.AbstractContainer;

import java.security.SecureRandom;

/**
 * Container with a single Zookeeper node
 */
public class ZookeeperContainer extends AbstractContainer {

    private static final String ZOOKEEPER_IMAGE = "jplock/zookeeper";

    public ZookeeperContainer(DockerClient dockerClient) {
        super(dockerClient);
    }

    @Override
    protected void pullImage() {
        pullImage(ZOOKEEPER_IMAGE, "latest");
    }

    @Override
    protected CreateContainerCmd dockerCommand() {
        return dockerClient
                .createContainerCmd(ZOOKEEPER_IMAGE)
                .withName("zookeeper_" + new SecureRandom().nextInt());
    }
}
