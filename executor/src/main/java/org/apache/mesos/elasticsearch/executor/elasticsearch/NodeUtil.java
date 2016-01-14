package org.apache.mesos.elasticsearch.executor.elasticsearch;

import org.apache.log4j.Logger;
import org.elasticsearch.node.Node;

import java.util.concurrent.ExecutionException;

/**
 */
public class NodeUtil {
    Logger LOG = Logger.getLogger(NodeUtil.class);

    public String getNodeStatus(Node node) {
        try {
            String status = node.client().admin().cluster().prepareHealth().execute().get().getStatus().toString().toLowerCase();
            LOG.debug("ES status: " + status);
            return status;
        } catch (InterruptedException | ExecutionException e) {
            LOG.warn("ES not started. Retrying...", e);
            return "";
        }
    }
}
