package org.apache.mesos.elasticsearch.systemtest.callbacks;

import com.jayway.awaitility.Awaitility;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.log4j.Logger;
import org.apache.mesos.elasticsearch.systemtest.ESTasks;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Reponse which returns Zookeeper configuration of Elasticsearch
 */
public class ElasticsearchZookeeperResponse {
    private static final Logger LOGGER = Logger.getLogger(ElasticsearchZookeeperResponse.class);
    private final ESTasks esTasks;
    private String host;

    public ElasticsearchZookeeperResponse(ESTasks esTasks) {
        this.esTasks = esTasks;
        LOGGER.debug("Polling ES for zookeeper settings.");
        Awaitility.await().pollInterval(1L, TimeUnit.SECONDS).atMost(2L, TimeUnit.MINUTES).until(new ElasticsearchZookeeperCall());
    }

    public String getHost() {
        return "zk://" + host.replace("/mesos", "");
    }

    private class ElasticsearchZookeeperCall implements Callable<Boolean> {
        @Override
        public Boolean call() throws Exception {
            try {
                String url = "http://" + esTasks.getTasks().get(0).getString("http_address");
                LOGGER.debug("Querying: " + url);
                JSONObject nodes = Unirest.get(url + "/_nodes").asJson().getBody().getObject().getJSONObject("nodes");
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
            } catch (UnirestException | JSONException e) {
                return false;
            }
        }
    }
}
