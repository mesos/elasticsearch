package org.apache.mesos.elasticsearch.common.cli;

import com.beust.jcommander.Parameter;
import org.apache.mesos.elasticsearch.common.cli.validators.CLIValidators;

/**
 * Class to reuse ZooKeeper CLI Parameters
 */
public class ZookeeperCLIParameter {
    public static final String ZOOKEEPER_MESOS_URL = "--zookeeperMesosUrl";
    @Parameter(names = {ZOOKEEPER_MESOS_URL}, required = true, description = "Zookeeper urls for Mesos in the format zk://IP:PORT,IP:PORT,...)", validateWith = CLIValidators.NotEmptyString.class)
    private String zookeeperMesosUrl = "zk://mesos.master:2181";
    public String getZookeeperMesosUrl() {
        return zookeeperMesosUrl;
    }

    public static final String ZOOKEEPER_MESOS_TIMEOUT = "--zookeeperMesosTimeout";
    @Parameter(names = {ZOOKEEPER_MESOS_TIMEOUT}, description = "The timeout for connecting to zookeeper for Mesos (ms).", validateValueWith = CLIValidators.PositiveLong.class)
    private long zookeeperMesosTimeout = 20000L;
    public long getZookeeperMesosTimeout() {
        return zookeeperMesosTimeout;
    }

    public static final String ZOOKEEPER_FRAMEWORK_URL = "--zookeeperFrameworkUrl";
    @Parameter(names = {ZOOKEEPER_FRAMEWORK_URL}, required = false, description = "Zookeeper urls for the framework in the format zk://IP:PORT,IP:PORT,...)", validateWith = CLIValidators.NotEmptyString.class)
    private String zookeeperFrameworkUrl = "zk://mesos.master:2181";
    public String getZookeeperFrameworkUrl() {
        return zookeeperFrameworkUrl;
    }

    public static final String ZOOKEEPER_FRAMEWORK_TIMEOUT = "--zookeeperFrameworkTimeout";
    @Parameter(names = {ZOOKEEPER_FRAMEWORK_TIMEOUT}, required = false, description = "The timeout for connecting to zookeeper for the framework (ms).", validateValueWith = CLIValidators.PositiveLong.class)
    private long zookeeperFrameworkTimeout = 20000L;
    public long getZookeeperFrameworkTimeout() {
        return zookeeperFrameworkTimeout;
    }

}
