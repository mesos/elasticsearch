package org.apache.mesos.elasticsearch.executor.elasticsearch;

import org.elasticsearch.common.settings.Settings;

/**
 * Builds the ES settings from the provided settings.
 */
public class ElasticsearchSettings {
    public Settings.Builder defaultSettings() {
        return Settings.settingsBuilder()
                .put("path.home", "/usr/share/elasticsearch")
                .put("node.local", false) // Can never be local.
                .put("discovery.type", "zen")
                .put("discovery.zen.ping.multicast.enabled", false);
    }
}
