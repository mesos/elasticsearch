package org.apache.mesos.elasticsearch.executor.elasticsearch;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

/**
 * Launches an elasticsearch node
 */
public class ElasticsearchLauncher {
    private final Settings settings;

    public ElasticsearchLauncher(ElasticsearchSettings settings) {
        this.settings = settings.build();
    }

    public Node launch() {
        return NodeBuilder.nodeBuilder().settings(settings).node();
    }
}
