package org.apache.mesos.elasticsearch.systemtest.callbacks;

import com.jayway.awaitility.Awaitility;
import com.mashape.unirest.http.Unirest;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import java.util.concurrent.TimeUnit;

/**
 * Reponse which returns Zookeeper configuration of Elasticsearch
 */
public class ElasticsearchZookeeperResponse {
    private static final Logger LOGGER = Logger.getLogger(ElasticsearchZookeeperResponse.class);
    private String host;

    public ElasticsearchZookeeperResponse(String nodeAddress) {
        LOGGER.debug("Polling ES for zookeeper settings.");
        Awaitility.await().pollInterval(1L, TimeUnit.SECONDS).atMost(2L, TimeUnit.MINUTES).until(() -> {
            try {
                JSONObject nodes = Unirest.get("http://" + nodeAddress + "/_nodes").asJson().getBody().getObject().getJSONObject("nodes");
                String nodeKey = (String) nodes.keys().next();
                JSONObject node = nodes.getJSONObject(nodeKey);
                host = node
                        .getJSONObject("settings")
                        .getJSONObject("sonian")
                        .getJSONObject("elasticsearch")
                        .getJSONObject("zookeeper")
                        .getJSONObject("client")
                        .getString("host");
                return true;
            } catch (Exception e) {
                return false;
            }
        });

    }

    public String getHost() {
        return "zk://" + host.replace("/mesos", "");
    }
}
