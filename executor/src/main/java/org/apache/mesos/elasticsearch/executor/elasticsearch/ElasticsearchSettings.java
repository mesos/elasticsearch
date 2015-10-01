package org.apache.mesos.elasticsearch.executor.elasticsearch;

import org.elasticsearch.common.settings.ImmutableSettings;

/**
 * Builds the ES settings from the provided settings.
 */
public class ElasticsearchSettings {
    public ImmutableSettings.Builder defaultSettings() {
        return ImmutableSettings.settingsBuilder()
                .put("node.local", false) // Can never be local. Requires use of remote zookeeper.
                .put("discovery.type", "com.sonian.elasticsearch.zookeeper.discovery.ZooKeeperDiscoveryModule")
                .put("sonian.elasticsearch.zookeeper.settings.enabled", true)
                .put("sonian.elasticsearch.zookeeper.discovery.state_publishing.enabled", true);
    }
}
