package org.apache.mesos.elasticsearch.performancetest;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.jayway.awaitility.Awaitility;
import org.apache.mesos.mini.container.AbstractContainer;

import java.security.SecureRandom;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;

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
        return dockerClient.logContainerCmd(getContainerId()).withStdOut().exec();
    }
}
