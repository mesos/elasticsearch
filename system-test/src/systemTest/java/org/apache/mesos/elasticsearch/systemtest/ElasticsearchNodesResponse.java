package org.apache.mesos.elasticsearch.systemtest;

import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.json.JSONObject;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static com.jayway.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.is;

/**
 * Response for Elasticsearch nodes
 */
public class ElasticsearchNodesResponse {

    private List<JSONObject> tasks;

    private int nodesCount;

    public ElasticsearchNodesResponse(List<JSONObject> tasks, int nodesCount) {
        this.tasks = tasks;
        this.nodesCount = nodesCount;
        await().atMost(5, TimeUnit.MINUTES).pollInterval(1, TimeUnit.SECONDS).until(new ElasticsearchNodesCall(), is(true));
    }

    private boolean discoverySuccessful;

    class ElasticsearchNodesCall implements Callable<Boolean> {

        @Override
        public Boolean call() throws Exception {
            try {
                for (JSONObject task : tasks) {
                    if (!(Unirest.get("http://" + task.getString("http_address") + "/_nodes").asJson().getBody().getObject().getJSONObject("nodes").length() == nodesCount)) {
                        discoverySuccessful = false;
                    }
                }
                discoverySuccessful = true;
                return true;
            } catch (UnirestException e) {
                DiscoverySystemTest.LOGGER.info("Polling Elasticsearch _nodes endpoints...");
                return false;
            }
        }
    }

    public boolean isDiscoverySuccessful() {
        return discoverySuccessful;
    }
}
