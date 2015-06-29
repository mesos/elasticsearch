package org.apache.mesos.elasticsearch.common;

/**
 * Represents an error in the ZK address parsing.
 */
public class ZooKeeperAddressException extends IllegalArgumentException {
    public ZooKeeperAddressException(String zkUrl) {
        super(String.format("Invalid zk url format: '%s'. Expected '%s'", zkUrl, ZooKeeper.VALID_ZK_URL));
    }
}
