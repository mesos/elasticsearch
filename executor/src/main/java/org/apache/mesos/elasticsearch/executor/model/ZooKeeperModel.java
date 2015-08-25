package org.apache.mesos.elasticsearch.executor.model;

import org.elasticsearch.common.settings.ImmutableSettings;

/**
 * Model representing ZooKeeper information
 */
public class ZooKeeperModel implements RunTimeSettings {
    public static final String ZOOKEEPER_ADDRESS_KEY = "sonian.elasticsearch.zookeeper.client.host";
    private final String address;

    public ZooKeeperModel(String address) {
        this.address = address;
    }

    private ImmutableSettings.Builder getAddress() {
        return ImmutableSettings.settingsBuilder().put(ZOOKEEPER_ADDRESS_KEY, address);
    }

    @Override
    public ImmutableSettings.Builder getRuntimeSettings() {
        return ImmutableSettings.settingsBuilder().put(getAddress().build());
    }
}
