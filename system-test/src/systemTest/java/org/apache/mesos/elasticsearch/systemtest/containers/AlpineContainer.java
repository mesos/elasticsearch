package org.apache.mesos.elasticsearch.systemtest.containers;

import com.containersol.minimesos.container.AbstractContainer;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.Volume;

import java.security.SecureRandom;

/**
 * A wrapper for an alpine data container
 */
public class AlpineContainer extends AbstractContainer {

    public static final String ALPINE_IMAGE_NAME = "alpine";
    private final String hostVolume;
    private final String containerVolume;
    private final String[] cmd;

    public AlpineContainer(DockerClient dockerClient, String hostVolume, String containerVolume, String... cmd) {
        super(dockerClient);
        this.hostVolume = hostVolume;
        this.containerVolume = containerVolume;
        this.cmd = cmd;
    }

    @Override
    protected void pullImage() {
        pullImage(ALPINE_IMAGE_NAME, "latest");
    }

    @Override
    protected CreateContainerCmd dockerCommand() {
        return dockerClient
                .createContainerCmd(ALPINE_IMAGE_NAME)
                .withName("Alpine" + "_" + new SecureRandom().nextInt())
                .withBinds(new Bind(hostVolume, new Volume(containerVolume)))
                .withAttachStdout(true)
                .withAttachStderr(true)
                .withCmd(cmd);
    }

    @Override
    public String getRole() {
        return "Alpine";
    }

}
