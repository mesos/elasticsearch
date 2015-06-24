package org.apache.mesos.elasticsearch.executor.elasticsearch;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

/**
 * Launches an elasticsearch node
 */
public class ElasticsearchLauncher implements Launcher {
    private ImmutableSettings.Builder settings;

    public ElasticsearchLauncher(ImmutableSettings.Builder newSettings) {
        if (newSettings == null) {
            throw new NullPointerException("Settings cannot be null");
        }
        this.settings = newSettings;
    }

    @Override
    public Node launch() {
        return NodeBuilder.nodeBuilder().settings(settings.build()).node();
    }

    @Override
    public void addRuntimeSettings(ImmutableSettings.Builder settingsToAdd) {
        if (settingsToAdd != null) {
            this.settings.put(settingsToAdd.build());
        }
    }
}
