package org.apache.mesos.elasticsearch.executor.elasticsearch;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

/**
 * Launches an elasticsearch node
 */
public class ElasticsearchLauncher implements Launcher {
    private Settings settings;

    public ElasticsearchLauncher(ImmutableSettings.Builder settings) {
        this.settings = settings.build();
    }

    @Override
    public Node launch() {
        return NodeBuilder.nodeBuilder().settings(settings).node();
    }

    @Override
    public void addRuntimeSettings(ImmutableSettings.Builder settings) {
        settings.put(settings);
    }
}
