package org.apache.mesos.elasticsearch.common;

import java.util.List;

/**
 * Provides the ZooKeeper address(es) in a format required by Mesos
 */
public class MesosStateZKFormatter {

    private final String address;

    public MesosStateZKFormatter(String zkUrl) {
        List<ZooKeeperAddress> addressList = ZooKeeperAddressParser.validateZkUrl(zkUrl);
        StringBuilder builder = new StringBuilder();
        addressList.forEach(add -> builder.append("," + add.getAddress() + ":" + add.getPort()));
        builder.deleteCharAt(0);
        address = builder.toString();
    }

    /**
     * Get the ZooKeeper address, correctly formatted.
     *
     * @return the zookeeper addresses in the format host:port[,host:port,...]
     */
    public String getAddress() {
        return address;
    }
}
