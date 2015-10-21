package org.apache.mesos.elasticsearch.systemtest;

import com.containersol.minimesos.mesos.MesosSlave;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import org.apache.commons.lang.StringUtils;
import org.apache.mesos.elasticsearch.common.cli.ElasticsearchCLIParameter;
import org.apache.mesos.elasticsearch.common.cli.ZookeeperCLIParameter;
import org.apache.mesos.elasticsearch.scheduler.Configuration;
import com.containersol.minimesos.container.AbstractContainer;

import java.security.SecureRandom;
import java.util.stream.IntStream;

/**
 * Container for the Elasticsearch scheduler
 */
public class ElasticsearchSchedulerContainer extends AbstractContainer {

    private static final org.apache.mesos.elasticsearch.systemtest.Configuration TEST_CONFIG = new org.apache.mesos.elasticsearch.systemtest.Configuration();

    private final String zkIp;

    private String frameworkRole;

    private String zookeeperFrameworkUrl;
    private String dataDirectory;

    protected ElasticsearchSchedulerContainer(DockerClient dockerClient, String zkIp) {
        super(dockerClient);
        this.zkIp = zkIp;
        this.frameworkRole = "*"; // The default
    }

    protected ElasticsearchSchedulerContainer(DockerClient dockerClient, String zkIp, String frameworkRole) {
        super(dockerClient);
        this.zkIp = zkIp;
        this.frameworkRole = frameworkRole;
    }

    @Override
    protected void pullImage() {
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
