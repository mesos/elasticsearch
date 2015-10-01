package org.apache.mesos.elasticsearch.common.cli;

import com.beust.jcommander.Parameter;
import org.apache.mesos.elasticsearch.common.cli.validators.CLIValidators;

/**
 * Class to reuse ZooKeeper CLI Parameters
 */
public class ZookeeperCLIParameter {
    public static final String ZOOKEEPER_MESOS_URL = "--zookeeperMesosUrl";
    public static final String DEFAULT = "DEFAULT";
    public static final String ZOOKEEPER_MESOS_TIMEOUT = "--zookeeperMesosTimeout";
    public static final String ZOOKEEPER_FRAMEWORK_URL = "--zookeeperFrameworkUrl";
    public static final String ZOOKEEPER_FRAMEWORK_TIMEOUT = "--zookeeperFrameworkTimeout";
    @Parameter(names = {ZOOKEEPER_MESOS_URL}, required = true, description = "Zookeeper urls for Mesos in the format zk://IP:PORT,IP:PORT,...)", validateWith = CLIValidators.NotEmptyString.class)
    private String zookeeperMesosUrl = "zk://mesos.master:2181";
    @Parameter(names = {ZOOKEEPER_MESOS_TIMEOUT}, description = "The timeout for connecting to zookeeper for Mesos (ms).", validateValueWith = CLIValidators.PositiveLong.class)
    private long zookeeperMesosTimeout = 20000L;
    @Parameter(names = {ZOOKEEPER_FRAMEWORK_URL}, required = false, description = "Zookeeper urls for the framework in the format zk://IP:PORT,IP:PORT,...)", validateWith = CLIValidators.NotEmptyString.class)
    private String zookeeperFrameworkUrl = "";
    @Parameter(names = {ZOOKEEPER_FRAMEWORK_TIMEOUT}, required = false, description = "The timeout for connecting to zookeeper for the framework (ms).", validateValueWith = CLIValidators.PositiveLong.class)
    private long zookeeperFrameworkTimeout = 20000L;

    public String getZookeeperMesosUrl() {
        return zookeeperMesosUrl;
    }

    public long getZookeeperMesosTimeout() {
        return zookeeperMesosTimeout;
    }

    public String getZookeeperFrameworkUrl() {
        if (zookeeperFrameworkUrl.equals(DEFAULT)) {
            zookeeperFrameworkUrl = "";
        }
        return zookeeperFrameworkUrl;
    }

    public long getZookeeperFrameworkTimeout() {
        return zookeeperFrameworkTimeout;
    }

}
