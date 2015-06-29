package org.apache.mesos.elasticsearch.common;

import java.util.List;

/**
 * Formatter for native zookeeper connections
 */
public class ZooKeeperFormatter {
    private final String address;

    public ZooKeeperFormatter(String zkUrl) {
        List<ZooKeeperAddress> addressList = ZooKeeperAddressParser.validateZkUrl(zkUrl);
        address = zkUrl;
    }

    /**
     * Get the ZooKeeper address, correctly formatted.
     *
     * @return the zookeeper addresses in the format "zk://host:port/zkNode,..."
     */
    public String getAddress() {
        return address;
    }
}
