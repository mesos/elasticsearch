package org.apache.mesos.elasticsearch.executor.elasticsearch;

import org.elasticsearch.common.settings.ImmutableSettings;

/**
 * Builds the ES settings from the provided settings.
 */
public class ElasticsearchSettings {
    public ImmutableSettings.Builder defaultSettings() {
        return ImmutableSettings.settingsBuilder()
                .put("node.local", false) // Can never be local.
                .put("discovery.type", "zen")
                .put("discovery.zen.ping.multicast.enabled", false);
    }
}
