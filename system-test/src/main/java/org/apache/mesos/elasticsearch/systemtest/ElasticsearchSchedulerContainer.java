package org.apache.mesos.elasticsearch.systemtest;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import org.apache.mesos.elasticsearch.common.cli.ElasticsearchCLIParameter;
import org.apache.mesos.elasticsearch.common.cli.ZookeeperCLIParameter;
import org.apache.mesos.elasticsearch.scheduler.Configuration;
import org.apache.mesos.mini.container.AbstractContainer;

import java.security.SecureRandom;
import java.util.stream.IntStream;

/**
 * Container for the Elasticsearch scheduler
 */
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
                .withCmd(
                        ZookeeperCLIParameter.ZOOKEEPER_URL, "zk://" + mesosIp + ":2181/mesos",
                        ElasticsearchCLIParameter.ELASTICSEARCH_NODES, "3",
                        Configuration.ELASTICSEARCH_RAM, "256",
                        Configuration.WEB_UI_PORT, "31100",
                        Configuration.EXECUTOR_NAME, "esdemo");
    }
}
