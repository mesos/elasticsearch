package org.apache.mesos.elasticsearch.common.zookeeper.exception;

import org.apache.mesos.elasticsearch.common.zookeeper.model.ZKAddress;

/**
 * Represents an error in the ZK address parsing.
 */
public class ZKAddressException extends IllegalArgumentException {
    public ZKAddressException(String zkUrl) {
        super(String.format("Invalid zk url format: '%s'. Expected '%s'", zkUrl, ZKAddress.VALID_ZK_URL));
    }
}
