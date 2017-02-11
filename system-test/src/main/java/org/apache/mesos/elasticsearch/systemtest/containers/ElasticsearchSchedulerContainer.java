package org.apache.mesos.elasticsearch.systemtest.containers;

import com.containersol.minimesos.config.ContainerConfigBlock;
import com.containersol.minimesos.container.AbstractContainer;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import org.apache.mesos.elasticsearch.common.cli.ElasticsearchCLIParameter;
import org.apache.mesos.elasticsearch.common.cli.ZookeeperCLIParameter;
import org.apache.mesos.elasticsearch.scheduler.Configuration;

import java.security.SecureRandom;

import static org.apache.mesos.elasticsearch.systemtest.Configuration.getDocker0AdaptorIpAddress;

/**
 * Container for the Elasticsearch scheduler
 */
public class ElasticsearchSchedulerContainer extends AbstractContainer {

    private static final org.apache.mesos.elasticsearch.systemtest.Configuration TEST_CONFIG = new org.apache.mesos.elasticsearch.systemtest.Configuration();

    protected final String docker0AdaptorIpAddress;

    private final DockerClient dockerClient;

    private final String zkIp;

    private String frameworkRole;

    private final String dataDirectory;

    public ElasticsearchSchedulerContainer(DockerClient dockerClient, String zkIp) {
        this(dockerClient, zkIp, "*", Configuration.DEFAULT_HOST_DATA_DIR);
    }

    public ElasticsearchSchedulerContainer(DockerClient dockerClient, String zkIp, String dataDirectory) {
        this(dockerClient, zkIp, "*", dataDirectory);
    }

    public ElasticsearchSchedulerContainer(DockerClient dockerClient, String zkIp, String frameworkRole, String dataDir) {
        super(new ContainerConfigBlock(TEST_CONFIG.getSchedulerName(), "latest"));
        this.zkIp = zkIp;
        this.frameworkRole = frameworkRole;
        this.dataDirectory = dataDir;
        this.dockerClient = dockerClient;

        docker0AdaptorIpAddress = getDocker0AdaptorIpAddress();
    }

    @Override
    public void pullImage() {
        dockerClient.pullImageCmd(TEST_CONFIG.getSchedulerImageName());
    }

    @Override
    protected CreateContainerCmd dockerCommand() {
        return dockerClient
                .createContainerCmd(TEST_CONFIG.getSchedulerImageName())
                .withName(TEST_CONFIG.getSchedulerName() + "_" + new SecureRandom().nextInt())
                .withEnv("JAVA_OPTS=-Xms128m -Xmx256m")
                .withCmd(
                        ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, getZookeeperMesosUrl(),
                        ElasticsearchCLIParameter.ELASTICSEARCH_NODES, Integer.toString(TEST_CONFIG.getElasticsearchNodesCount()),
                        Configuration.ELASTICSEARCH_RAM, Integer.toString(TEST_CONFIG.getElasticsearchMemorySize()),
                        Configuration.ELASTICSEARCH_CPU, "0.1",
                        Configuration.ELASTICSEARCH_DISK, "150",
                        Configuration.USE_IP_ADDRESS, "false",
                        Configuration.WEB_UI_PORT, Integer.toString(TEST_CONFIG.getSchedulerGuiPort()),
                        Configuration.EXECUTOR_NAME, TEST_CONFIG.getElasticsearchJobName(),
                        Configuration.FRAMEWORK_USE_DOCKER, "true",
                        Configuration.DATA_DIR, dataDirectory,
                        Configuration.FRAMEWORK_ROLE, frameworkRole);
    }

    public String getZookeeperMesosUrl() {
        return "zk://" + zkIp + ":2181/mesos";
    }

    @Override
    public String getRole() {
        return TEST_CONFIG.getSchedulerName();
    }

}
