package org.apache.mesos.elasticsearch.executor.model;

import org.apache.log4j.Logger;
import org.elasticsearch.common.settings.ImmutableSettings;

/**
 * Model representing ZooKeeper information
 */
public class ZooKeeperModel implements RunTimeSettings {
    public static final String ZOOKEEPER_ADDRESS_KEY = "sonian.elasticsearch.zookeeper.client.host";
    private static final Logger LOGGER = Logger.getLogger(ZooKeeperModel.class);
    private final String address;
    private long timeout;

    public ZooKeeperModel(String address, long timeout) {
        this.address = address;
        this.timeout = timeout;
    }

    private ImmutableSettings.Builder getAddress() {
        LOGGER.debug(ZOOKEEPER_ADDRESS_KEY + ": " + address);
        return ImmutableSettings.settingsBuilder()
                .put(ZOOKEEPER_ADDRESS_KEY, address)
                .put("sonian.elasticsearch.zookeeper.client.session.timeout", timeout);
    }

    @Override
    public ImmutableSettings.Builder getRuntimeSettings() {
        return getAddress();
    }
}
