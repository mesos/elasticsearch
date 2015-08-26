package org.apache.mesos.elasticsearch.executor.model;

import org.apache.log4j.Logger;
import org.elasticsearch.common.settings.ImmutableSettings;

/**
 * Model representing ZooKeeper information
 */
public class ZooKeeperModel implements RunTimeSettings {
    private static final Logger LOGGER = Logger.getLogger(ZooKeeperModel.class);
    public static final String ZOOKEEPER_ADDRESS_KEY = "sonian.elasticsearch.zookeeper.client.host";
    private final String address;

    public ZooKeeperModel(String address) {
        this.address = address;
    }

    private ImmutableSettings.Builder getAddress() {
        LOGGER.debug(ZOOKEEPER_ADDRESS_KEY + ": " + address);
        return ImmutableSettings.settingsBuilder().put(ZOOKEEPER_ADDRESS_KEY, address);
    }

    @Override
    public ImmutableSettings.Builder getRuntimeSettings() {
        return getAddress();
    }
}
