package org.apache.mesos.elasticsearch.systemtest.callbacks;

import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.json.JSONObject;

/**
 * Reponse which returns Zookeeper configuration of Elasticsearch
 */
public class ElasticsearchZookeeperResponse {

    private final String host;

    public ElasticsearchZookeeperResponse(String nodeAddress) throws UnirestException {
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

    }

    public String getHost() {
        return "zk://" + host.replace("/mesos", "");
    }
}
