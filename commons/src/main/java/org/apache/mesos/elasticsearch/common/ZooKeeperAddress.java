package org.apache.mesos.elasticsearch.common;

import java.util.regex.Matcher;

/**
 * Model representing ZooKeeper address
 */
public class ZooKeeperAddress {
    private final String ipPort;
    private final String zkAddress;
    private String zkNode = "";

    /**
     * Creates an object representing the zookeeper address model.
     *
     * @param zookeeperAddress a String in the format "zk://host1:port1,host2:port2,.../path"
     */
    public ZooKeeperAddress(String zookeeperAddress) {
        Matcher zkMatcher = ZooKeeperAddressParser.validateZkUrl(zookeeperAddress);
        this.zkAddress = zookeeperAddress;
        this.ipPort = zkMatcher.group(1);
        setPath(zkMatcher.group(2));
    }

    private void setPath(String str) {
        if (str != null) {
            zkNode = str;
        }
    }

    // Returns the full zk address in the format "zk://host1:port1,host2:port2,.../path"
    public String getZkAddress() {
        return zkAddress;
    }

    // Returns the IP address and port number of the zookeeper instance in the format [addr]:[port]
    public String getIpPort() {
        return ipPort;
    }

    // Returns the zookeeper path
    public String getZKNode() {
        return zkNode;
    }
}
