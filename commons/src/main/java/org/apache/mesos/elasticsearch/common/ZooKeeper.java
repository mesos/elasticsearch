package org.apache.mesos.elasticsearch.common;

/**
 * Global zookeeper configuration
 */
public interface ZooKeeper {
    String ZOOKEEPER_ARG = "-zk";
    String VALID_ZK_URL = "zk://host1:port1,user:pass@host2:port2/path,.../path";
}
