package org.apache.mesos.elasticsearch.systemtest;

import com.containersol.minimesos.MesosCluster;
import com.containersol.minimesos.container.AbstractContainer;
import com.containersol.minimesos.mesos.MesosSlave;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import org.apache.mesos.elasticsearch.common.cli.ElasticsearchCLIParameter;
import org.apache.mesos.elasticsearch.common.cli.ZookeeperCLIParameter;
import org.apache.mesos.elasticsearch.scheduler.Configuration;

import java.security.SecureRandom;
import java.util.Arrays;
import java.util.List;

import static org.apache.mesos.elasticsearch.systemtest.Configuration.getDocker0AdaptorIpAddress;

/**
 * Container for the Elasticsearch scheduler
 */
public class ElasticsearchSchedulerContainer extends AbstractContainer {

    private static final org.apache.mesos.elasticsearch.systemtest.Configuration TEST_CONFIG = new org.apache.mesos.elasticsearch.systemtest.Configuration();
    protected final String docker0AdaptorIpAddress;

    private final String zkIp;

    private String frameworkRole;
    private final MesosCluster cluster;
    private final String dataDirectory;

    public ElasticsearchSchedulerContainer(DockerClient dockerClient, String zkIp, MesosCluster cluster) {
        this(dockerClient, zkIp, "*", cluster, Configuration.DEFAULT_HOST_DATA_DIR);
    }

    public ElasticsearchSchedulerContainer(DockerClient dockerClient, String zkIp, MesosCluster cluster, String dataDirectory) {
        this(dockerClient, zkIp, "*", cluster, dataDirectory);
    }

    public ElasticsearchSchedulerContainer(DockerClient dockerClient, String zkIp, String frameworkRole, MesosCluster cluster, String dataDir) {
        super(dockerClient);
        this.zkIp = zkIp;
        this.frameworkRole = frameworkRole;
        this.cluster = cluster;
        this.dataDirectory = dataDir;

        docker0AdaptorIpAddress = getDocker0AdaptorIpAddress(dockerClient);
    }

    @Override
    public void pullImage() {
        dockerClient.pullImageCmd(TEST_CONFIG.getSchedulerImageName());
    }

    @Override
    protected CreateContainerCmd dockerCommand() {
        List<MesosSlave> slaves = Arrays.asList(cluster.getSlaves());

        // Note we are redirecting each slave host to the static docker0 adaptor address (docker0AdaptorIpAddress).
        // The executors expose ports and when running system tests these are exposed on the single docker daemon machine
        // (localhost for linux, virtual machine for mac users). However, the docker0 ip address *always* points to the host.
        return dockerClient
                .createContainerCmd(TEST_CONFIG.getSchedulerImageName())
                .withName(TEST_CONFIG.getSchedulerName() + "_" + new SecureRandom().nextInt())
                .withEnv("JAVA_OPTS=-Xms128m -Xmx256m")
                .withExtraHosts(slaves.stream().map(mesosSlave -> mesosSlave.getHostname() + ":" + docker0AdaptorIpAddress).toArray(String[]::new))
                .withCmd(
                        ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, getZookeeperMesosUrl(),
                        ElasticsearchCLIParameter.ELASTICSEARCH_NODES, Integer.toString(TEST_CONFIG.getElasticsearchNodesCount()),
                        Configuration.ELASTICSEARCH_RAM, Integer.toString(TEST_CONFIG.getElasticsearchMemorySize()),
                        Configuration.ELASTICSEARCH_CPU, "0.1",
                        Configuration.ELASTICSEARCH_DISK, "150",
                        Configuration.USE_IP_ADDRESS, "true",
                        Configuration.WEB_UI_PORT, Integer.toString(TEST_CONFIG.getSchedulerGuiPort()),
                        Configuration.EXECUTOR_NAME, TEST_CONFIG.getElasticsearchJobName(),
                        Configuration.DATA_DIR, dataDirectory,
                        Configuration.FRAMEWORK_ROLE, frameworkRole);
    }

    public String getZookeeperMesosUrl() {
        return "zk://" + zkIp + ":2181/mesos";
    }
}
