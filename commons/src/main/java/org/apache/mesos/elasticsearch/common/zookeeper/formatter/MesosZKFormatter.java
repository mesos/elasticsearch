package org.apache.mesos.elasticsearch.common.zookeeper.formatter;

import org.apache.mesos.elasticsearch.common.zookeeper.exception.ZKAddressException;
import org.apache.mesos.elasticsearch.common.zookeeper.parser.ZKAddressParser;

/**
 * Provides the ZooKeeper address(es) in a format required by Mesos.
 *
 * The format consists of a list of ZK servers and a path
 *
 * Example: zk://host1:port1,host2:port2/mesos
 */
public class MesosZKFormatter extends AbstractZKFormatter {

    public MesosZKFormatter(ZKAddressParser parser) {
        super(parser);
    }

    /**
     * Validates the ZooKeeper address and returns it
     *
     * @param zkUrl The raw ZK address string in the format "zk://host:port/zkNode,..."
     *
     * @throws ZKAddressException if the raw zkURL is invalid.
     *
     * @return the zookeeper addresses in the format zk://host:port[,host:port,...]/mesos
     */
    public String format(String zkUrl) {
        parser.validateZkUrl(zkUrl);
        return zkUrl;
    }

}
