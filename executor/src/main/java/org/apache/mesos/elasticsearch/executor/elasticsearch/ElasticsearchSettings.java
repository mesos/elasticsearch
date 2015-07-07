package org.apache.mesos.elasticsearch.executor.elasticsearch;

import org.elasticsearch.common.settings.ImmutableSettings;

/**
 * Builds the ES settings from the provided settings.
 */
public class ElasticsearchSettings {
    // Todo: Make ES settings, settings.
    public ImmutableSettings.Builder defaultSettings() {
        return ImmutableSettings.settingsBuilder()
                .put("node.local", false)
                .put("cluster.name", "mesos-elasticsearch")
                .put("node.master", true)
                .put("node.data", true)
                .put("index.number_of_shards", 5)
                .put("index.number_of_replicas", 1)
                .put("discovery.type", "com.sonian.elasticsearch.zookeeper.discovery.ZooKeeperDiscoveryModule")
                .put("sonian.elasticsearch.zookeeper.settings.enabled", true)
                .put("sonian.elasticsearch.zookeeper.client.session.timeout", 60)
                .put("sonian.elasticsearch.zookeeper.discovery.state_publishing.enabled", true);
    }
}
