package org.apache.mesos.elasticsearch.systemtest.containers;

import com.containersol.minimesos.container.AbstractContainer;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.command.CreateContainerCmd;
import org.apache.mesos.elasticsearch.systemtest.callbacks.LogContainerTestCallback;

import java.security.SecureRandom;

/**
 * Data Pusher container implementation
 */
public class DataPusherContainer extends AbstractContainer {

    public String pusherImageName = "alexglv/es-pusher";

    public String slaveAddress;

    private ResultCallback callback = new LogContainerTestCallback();

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
    }

    @Override
    public void start() {
        super.start();
        dockerClient.logContainerCmd(getContainerId()).withStdOut().withStdErr().exec(callback);
    }

    public String getLogStreamStdOut() {
        return callback.toString();
    }
}
