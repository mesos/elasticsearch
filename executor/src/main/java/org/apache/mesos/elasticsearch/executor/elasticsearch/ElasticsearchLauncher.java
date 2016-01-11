package org.apache.mesos.elasticsearch.executor.elasticsearch;

import org.apache.log4j.Logger;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.util.concurrent.ExecutionException;

/**
 * Launches an elasticsearch node
 */
public class ElasticsearchLauncher implements Launcher {
    private Settings.Builder settings;
    private final Logger LOG = Logger.getLogger(ElasticsearchLauncher.class);

    public ElasticsearchLauncher(Settings.Builder newSettings) {
        if (newSettings == null) {
            throw new NullPointerException("Settings cannot be null");
        }
        this.settings = newSettings;
    }

    @Override
    public Node launch() {
        ESLoggerFactory.getRootLogger().setLevel("INFO");

        Node node = NodeBuilder.nodeBuilder().settings(settings.build()).node();
        try {
            LOG.debug("Elasticsearch node started");
            ActionFuture<NodesInfoResponse> nodesInfoResponseActionFuture = node.client().admin().cluster().nodesInfo(new NodesInfoRequest());
            LOG.debug("Node info:\n" + nodesInfoResponseActionFuture.get().toString());
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("Unable to get node info", e);
        }
        return node;
    }

    @Override
    public void addRuntimeSettings(Settings.Builder settingsToAdd) {
        if (settingsToAdd != null) {
            this.settings.put(settingsToAdd.build());
        }
    }

    @Override
    public String toString() {
        return "ES settings: " + settings.build().toDelimitedString(',');
    }
}
