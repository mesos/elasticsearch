package org.apache.mesos.elasticsearch.performancetest;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import org.apache.mesos.mini.container.AbstractContainer;

import java.security.SecureRandom;
import java.util.stream.IntStream;

/**
 * Container for the Elasticsearch scheduler
 */
@SuppressWarnings({"PMD.AvoidUsingHardCodedIP"})
public class ElasticsearchSchedulerContainer extends AbstractContainer {

    public static final String SCHEDULER_IMAGE = "mesos/elasticsearch-scheduler";

    public static final String SCHEDULER_NAME = "elasticsearch-scheduler";

    private String mesosIp;

    protected ElasticsearchSchedulerContainer(DockerClient dockerClient, String mesosIp) {
        super(dockerClient);
        this.mesosIp = mesosIp;
    }

    @Override
    protected void pullImage() {
        dockerClient.pullImageCmd(SCHEDULER_IMAGE);
    }

    @Override
    protected CreateContainerCmd dockerCommand() {
        return dockerClient
                .createContainerCmd(SCHEDULER_IMAGE)
                .withName(SCHEDULER_NAME + "_" + new SecureRandom().nextInt())
                .withEnv("JAVA_OPTS=-Xms128m -Xmx256m")
                .withExtraHosts(IntStream.rangeClosed(1, 3).mapToObj(value -> "slave" + value + ":" + mesosIp).toArray(String[]::new))
                .withCmd("-zk", "zk://" + mesosIp + ":2181/mesos", "-n", "3", "-ram", "256");
    }
}
