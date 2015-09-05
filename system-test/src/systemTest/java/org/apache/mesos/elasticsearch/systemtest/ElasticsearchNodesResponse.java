package org.apache.mesos.elasticsearch.systemtest;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.mashape.unirest.request.GetRequest;
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

    private boolean discoverySuccessful = false;

    class ElasticsearchNodesCall implements Callable<Boolean> {

        // Returns whether to stop polling
        // Queries the Elasticsearch nodes list https://www.elastic.co/guide/en/elasticsearch/reference/1.6/cluster-nodes-info.html
        // `discoverySuccessful` is set to `true` iff return value is `true`
        @Override
        public Boolean call() throws Exception {
                for (JSONObject task : tasks) {
                    if (!endpointIsOk(task)) {
                        DiscoverySystemTest.LOGGER.info("At least one endpoint is not yet OK; will try again.");
                        return false;
                    }
                }
                DiscoverySystemTest.LOGGER.info("All Elasticsearch endpoints succeeded");
                discoverySuccessful = true;
                return true;
        }

        // Returns `true` iff endpoint is OK.
        // Returns `false` iff endpoint is definitely not OK or it is not yet determined and we should continue polling.
        private boolean endpointIsOk(JSONObject task) {
            String url = "http://" + task.getString("http_address") + "/_nodes";

            GetRequest request = Unirest.get(url);

            HttpResponse<String> response = null;
            try {
                response = request.asString();
            }
            catch (UnirestException e) {
                DiscoverySystemTest.LOGGER.info("Polling Elasticsearch endpoint '" + url + "' threw exception: " + e.getMessage());
                return false;
            }
            // response != null

            if (200 <= response.getStatus() && response.getStatus() < 400) {
                JsonNode body = null;

                try {
                    body = new JsonNode(response.getBody());
                } catch (RuntimeException e) {
                    DiscoverySystemTest.LOGGER.info("Polling Elasticsearch endpoint '" + url + "' returned bad response body: " + e.getMessage());
                    return false;
                }
                // body != null

                if (body.getObject().getJSONObject("nodes").length() != nodesCount) {
                    DiscoverySystemTest.LOGGER.info("Polling Elasticsearch endpoint '" + url + "' returned wrong number of nodes (Expected " + nodesCount + " but got " + body.getObject().getJSONObject("nodes").length() + ")");
                    return false;
                }
                else {
                    DiscoverySystemTest.LOGGER.info("Polling Elasticsearch endpoint '" + url + "' succeeded");
                    return true;
                }
            }
            else {
                DiscoverySystemTest.LOGGER.info("Polling Elasticsearch endpoint '" + url + "' returned bad status: " + response.getStatus() + " " + response.getStatusText());
                return false;
            }
        }
    }

    public boolean isDiscoverySuccessful() {
        return discoverySuccessful;
    }
}
