package org.apache.mesos.elasticsearch.common.zookeeper.formatter;

import org.apache.mesos.elasticsearch.common.zookeeper.exception.ZKAddressException;
import org.apache.mesos.elasticsearch.common.zookeeper.parser.ZKAddressParser;

/**
 * Formatter for native zookeeper connections
 *
 * The format consists of a list full Zookeeper URL
 *
 * Example: zk://host1:port1,host2:port2/mesos
 */
public class ZooKeeperFormatter extends AbstractZKFormatter {

    public ZooKeeperFormatter(ZKAddressParser parser) {
        super(parser);
    }

    /**
     * Get the ZooKeeper address, correctly formatted.
     * @param zkUrl The raw ZK address string in the format "zk://host:port/zkNode,..."
     * @return the zookeeper addresses in the format "zk://host:port/zkNode,..."
     * @throws ZKAddressException if the raw zkURL is invalid.
     */
    public String format(String zkUrl) {
        parser.validateZkUrl(zkUrl);
        return zkUrl;
    }
}
