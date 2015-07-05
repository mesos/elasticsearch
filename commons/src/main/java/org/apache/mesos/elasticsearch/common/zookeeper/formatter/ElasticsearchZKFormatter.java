package org.apache.mesos.elasticsearch.common.zookeeper.formatter;

import org.apache.mesos.elasticsearch.common.zookeeper.exception.ZKAddressException;
import org.apache.mesos.elasticsearch.common.zookeeper.parser.ZKAddressParser;

/**
 * Provides the ZooKeeper address in a format required by the Elasticsearch Zookeeper plugin.
 *
 * The format consists of a list of Zookeeper servers and a path.
 *
 * Example: host1:port1,host2:port2/mesos
 */
public class ElasticsearchZKFormatter extends AbstractZKFormatter {

    public ElasticsearchZKFormatter(ZKAddressParser parser) {
        super(parser);
    }

    /**
     * Get the ZooKeeper address, correctly formatted.
     *
     * @param zkUrl The raw ZK address string in the format "zk://host:port/zkNode,..."
     * @return the zookeeper addresses in the format "zk://host:port/zkNode,..."
     * @throws ZKAddressException if the raw zkURL is invalid.
     */
    @Override
    public String format(String zkUrl) {
        parser.validateZkUrl(zkUrl);
        return zkUrl.replace("zk://", "");
    }
}
