package org.apache.mesos.elasticsearch.systemtest;

import com.containersol.minimesos.MesosCluster;
import com.containersol.minimesos.container.AbstractContainer;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import org.apache.commons.lang.StringUtils;
import org.apache.mesos.elasticsearch.common.cli.ElasticsearchCLIParameter;
import org.apache.mesos.elasticsearch.common.cli.ZookeeperCLIParameter;
import org.apache.mesos.elasticsearch.scheduler.Configuration;

import java.security.SecureRandom;
import java.util.stream.IntStream;

/**
 * Container for the Elasticsearch scheduler
 */
public class ElasticsearchSchedulerContainer extends AbstractContainer {

    public static final String SCHEDULER_IMAGE = "mesos/elasticsearch-scheduler";

    public static final String SCHEDULER_NAME = "elasticsearch-scheduler";

    private MesosCluster mesosCluster;

    protected String zookeeperIp;

    private String frameworkRole;

    private String zookeeperFrameworkUrl;

    private String dataDirectory;

    protected ElasticsearchSchedulerContainer(DockerClient dockerClient, MesosCluster mesosCluster) {
        super(dockerClient);
        this.zookeeperIp = mesosCluster.getZkContainer().getIpAddress();
        this.mesosCluster = mesosCluster;
        this.frameworkRole = "*"; // The default
    }

    protected ElasticsearchSchedulerContainer(DockerClient dockerClient, String zookeeperId, String frameworkRole) {
        super(dockerClient);
        this.zookeeperIp = zookeeperId;
        this.frameworkRole = frameworkRole;
    }

    @Override
    protected void pullImage() {
        dockerClient.pullImageCmd(SCHEDULER_IMAGE);
    }

    @Override
    protected CreateContainerCmd dockerCommand() {
        CreateContainerCmd dockerCommand = dockerClient
                .createContainerCmd(SCHEDULER_IMAGE)
                .withName(SCHEDULER_NAME + "_" + new SecureRandom().nextInt())
                .withEnv("JAVA_OPTS=-Xms128m -Xmx256m")
                .withCmd(
                        ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, getZookeeperMesosUrl(),
                        ZookeeperCLIParameter.ZOOKEEPER_FRAMEWORK_URL, getZookeeperFrameworkUrl(),
                        ZookeeperCLIParameter.ZOOKEEPER_FRAMEWORK_TIMEOUT, "30000",
                        ElasticsearchCLIParameter.ELASTICSEARCH_NODES, "3",
                        Configuration.ELASTICSEARCH_RAM, "256",
                        Configuration.WEB_UI_PORT, "31100",
                        Configuration.EXECUTOR_NAME, "esdemo",
                        Configuration.DATA_DIR, getDataDirectory(),
                        Configuration.FRAMEWORK_ROLE, frameworkRole
                );

        for (AbstractContainer slave : mesosCluster.getSlaves()) {
            dockerCommand.withExtraHosts(IntStream.rangeClosed(1, 3).mapToObj(value -> "slave" + value + ":" + slave.getIpAddress()).toArray(String[]::new));
        }

        return dockerCommand;
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
        return "zk://" + zookeeperIp + ":2181/mesos";
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
