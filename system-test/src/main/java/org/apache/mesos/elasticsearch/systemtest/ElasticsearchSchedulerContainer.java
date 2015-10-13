package org.apache.mesos.elasticsearch.systemtest;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import org.apache.commons.lang.StringUtils;
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

    protected String mesosIp;

    private String frameworkName;

    private String frameworkRole;

    private String zookeeperFrameworkUrl;

    private String dataDirectory;

    protected ElasticsearchSchedulerContainer(DockerClient dockerClient, String mesosIp) {
        super(dockerClient);
        this.mesosIp = mesosIp;
        this.frameworkRole = "*"; // The default
        this.frameworkName = "elasticsearch";
    }

    protected ElasticsearchSchedulerContainer(DockerClient dockerClient, String mesosIp, String frameworkRole) {
        super(dockerClient);
        this.mesosIp = mesosIp;
        this.frameworkRole = frameworkRole;
        this.frameworkName = "elasticsearch";
    }

    protected ElasticsearchSchedulerContainer(DockerClient dockerClient, String mesosIp, String frameworkRole, String frameworkName) {
        super(dockerClient);
        this.mesosIp = mesosIp;
        this.frameworkRole = frameworkRole;
        this.frameworkName = frameworkName;
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
                .withExposedPorts(ExposedPort.tcp(5005)).withPortBindings(PortBinding.parse("5005:5005"))
                .withEnv("JAVA_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 -Xms128m -Xmx256m")
                .withExtraHosts(IntStream.rangeClosed(1, 3).mapToObj(value -> "slave" + value + ":" + mesosIp).toArray(String[]::new))
                .withCmd(
                        ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, getZookeeperMesosUrl(),
                        ZookeeperCLIParameter.ZOOKEEPER_FRAMEWORK_URL, getZookeeperFrameworkUrl(),
                        ZookeeperCLIParameter.ZOOKEEPER_FRAMEWORK_TIMEOUT, "30000",
                        ElasticsearchCLIParameter.ELASTICSEARCH_NODES, "3",
                        Configuration.ELASTICSEARCH_RAM, "256",
                        Configuration.WEB_UI_PORT, "31100",
                        Configuration.EXECUTOR_NAME, "esdemo",
                        Configuration.DATA_DIR, getDataDirectory(),
                        Configuration.FRAMEWORK_ROLE, frameworkRole,
                        Configuration.FRAMEWORK_NAME, frameworkName
                );
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
        return "zk://" + mesosIp + ":2181/mesos";
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
