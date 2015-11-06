package org.apache.mesos.elasticsearch.systemtest;

import com.containersol.minimesos.MesosCluster;
import com.containersol.minimesos.container.AbstractContainer;
import com.containersol.minimesos.mesos.MesosSlave;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import org.apache.commons.lang.StringUtils;
import org.apache.mesos.elasticsearch.common.cli.ElasticsearchCLIParameter;
import org.apache.mesos.elasticsearch.common.cli.ZookeeperCLIParameter;
import org.apache.mesos.elasticsearch.scheduler.Configuration;

import java.security.SecureRandom;
import java.util.Arrays;
import java.util.List;

/**
 * Container for the Elasticsearch scheduler
 */
@SuppressWarnings("PMD.AvoidUsingHardCodedIP")
public class ElasticsearchSchedulerContainer extends AbstractContainer {

    private static final org.apache.mesos.elasticsearch.systemtest.Configuration TEST_CONFIG = new org.apache.mesos.elasticsearch.systemtest.Configuration();
    protected final String DOCKER0_ADAPTOR_IP_ADDRESS;

    private final String zkIp;

    private String frameworkRole;
    private final MesosCluster cluster;

    private String zookeeperFrameworkUrl;
    private String dataDirectory;

    public ElasticsearchSchedulerContainer(DockerClient dockerClient, String zkIp, MesosCluster cluster) {
        this(dockerClient, zkIp, "*", cluster);
    }

    public ElasticsearchSchedulerContainer(DockerClient dockerClient, String zkIp, String frameworkRole, MesosCluster cluster) {
        super(dockerClient);
        this.zkIp = zkIp;
        this.frameworkRole = frameworkRole;
        this.cluster = cluster;

        DOCKER0_ADAPTOR_IP_ADDRESS = dockerClient.versionCmd().exec().getVersion().startsWith("1.9.") ? "172.17.0.1" : "172.17.42.1";
    }

    @Override
    public void pullImage() {
        dockerClient.pullImageCmd(TEST_CONFIG.getSchedulerImageName());
    }

    @Override
    protected CreateContainerCmd dockerCommand() {
        List<MesosSlave> slaves = Arrays.asList(cluster.getSlaves());

        // Note we are redirecting each slave host to the static docker0 adaptor address (DOCKER0_ADAPTOR_IP_ADDRESS).
        // The executors expose ports and when running system tests these are exposed on the single docker daemon machine
        // (localhost for linux, virtual machine for mac users). However, the docker0 ip address *always* points to the host.
        return dockerClient
                .createContainerCmd(TEST_CONFIG.getSchedulerImageName())
                .withName(TEST_CONFIG.getSchedulerName() + "_" + new SecureRandom().nextInt())
                .withEnv("JAVA_OPTS=-Xms128m -Xmx256m")
                .withExtraHosts(slaves.stream().map(mesosSlave -> mesosSlave.getHostname() + ":" + DOCKER0_ADAPTOR_IP_ADDRESS).toArray(String[]::new))
                .withCmd(
                        ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, getZookeeperMesosUrl(),
                        ZookeeperCLIParameter.ZOOKEEPER_FRAMEWORK_URL, getZookeeperFrameworkUrl(),
                        ZookeeperCLIParameter.ZOOKEEPER_FRAMEWORK_TIMEOUT, "30000",
                        ElasticsearchCLIParameter.ELASTICSEARCH_NODES, Integer.toString(TEST_CONFIG.getElasticsearchNodesCount()),
                        Configuration.ELASTICSEARCH_RAM, Integer.toString(TEST_CONFIG.getElasticsearchMemorySize()),
                        Configuration.WEB_UI_PORT, Integer.toString(TEST_CONFIG.getSchedulerGuiPort()),
                        Configuration.EXECUTOR_NAME, TEST_CONFIG.getElasticsearchJobName(),
                        Configuration.DATA_DIR, getDataDirectory(),
                        Configuration.FRAMEWORK_ROLE, frameworkRole);
    }

    private String getDataDirectory() {
        if (dataDirectory == null) {
            return Configuration.DEFAULT_HOST_DATA_DIR;
        } else {
            return dataDirectory;
        }
    }

    public void setDataDirectory(String dataDirectory) {
        this.dataDirectory = dataDirectory;
    }

    public String getZookeeperMesosUrl() {
        return "zk://" + zkIp + ":2181/mesos";
    }

    public String getZookeeperFrameworkUrl() {
      if (StringUtils.isBlank(zookeeperFrameworkUrl)) {
        return getZookeeperMesosUrl();
      } else {
        return zookeeperFrameworkUrl;
      }
    }

    public void setZookeeperFrameworkUrl(String zookeeperFrameworkUrl) {
        this.zookeeperFrameworkUrl = zookeeperFrameworkUrl;
    }
}
