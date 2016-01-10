package org.apache.mesos.elasticsearch.executor.elasticsearch;

import org.elasticsearch.common.settings.Settings;

import static org.apache.mesos.elasticsearch.common.elasticsearch.ElasticsearchSettings.*;

/**
 * Builds the ES settings from the provided settings.
 */
public class ElasticsearchSettings {

    // Todo (pnw): Migrate to es settings. No point having them here when they are static
    public Settings.Builder defaultSettings() {
        return Settings.settingsBuilder()
                .put("path.home", CONTAINER_PATH_HOME)
                .put("path.data", CONTAINER_DATA_VOLUME)
                .put("path.logs", CONTAINER_PATH_LOGS)
                .put("gateway.expected_nodes", 1)
                .put("gateway.recover_after_nodes", 1)
                .put("index.number_of_replicas", 0)
                .put("index.auto_expand_replicas", "0-all")
                .put("network.bind_host", "0.0.0.0") // For ES 2.0.0, this is required. Without, it doesn't appear to expose properly.
                .put("network.publish_host", "_non_loopback:ipv4_") // Prevent ES from publishing the lo interface as it's IP.
                .put("node.local", false) // Can never be local.
                .put("discovery.type", "zen")
                .put("discovery.zen.ping.multicast.enabled", false);
    }
}
