package org.apache.mesos.elasticsearch.common.cli;

import com.beust.jcommander.Parameter;
import org.apache.mesos.elasticsearch.common.cli.validators.CLIValidators;

/**
 * Class to reuse ZooKeeper CLI Parameters
 */
public class ZookeeperCLIParameter {
    public static final String ZOOKEEPER_URL = "--zookeeperUrl";
    @Parameter(names = {ZOOKEEPER_URL}, required = true, description = "Zookeeper urls in the format zk://IP:PORT,IP:PORT,...)", validateWith = CLIValidators.NotEmptyString.class)
    private String zookeeperUrl = "zk://mesos.master:2181";
    public String getZookeeperUrl() {
        return zookeeperUrl;
    }

    public static final String ZOOKEEPER_TIMEOUT = "--zookeeperTimeout";
    @Parameter(names = {ZOOKEEPER_TIMEOUT}, description = "The timeout for connecting to zookeeper (ms).", validateValueWith = CLIValidators.PositiveLong.class)
    private long zookeeperTimeout = 20000L;
    public long getZookeeperTimeout() {
        return zookeeperTimeout;
    }

    public static final String ZOOKEEPER_FRAMEWORK_URL = "--zookeeperFrameworkUrl";
    @Parameter(names = {ZOOKEEPER_FRAMEWORK_URL}, required = false, description = "Zookeeper urls for the framework in the format zk://IP:PORT,IP:PORT,...)", validateWith = CLIValidators.NotEmptyString.class)
    private String zookeeperFrameworkUrl = "zk://mesos.master:2181";
    public String getZookeeperFrameworkUrl() {
        return zookeeperFrameworkUrl;
    }

}
