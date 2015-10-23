package org.apache.mesos.elasticsearch.systemtest.callbacks;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.mashape.unirest.request.GetRequest;
import org.apache.log4j.Logger;
import org.apache.mesos.elasticsearch.systemtest.ESTasks;
import org.json.JSONObject;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static com.jayway.awaitility.Awaitility.await;

/**
 * Response for Elasticsearch nodes. No need for discovery test.
 */
public class ElasticsearchNodesResponse {
    private static final Logger LOGGER = Logger.getLogger(ElasticsearchNodesResponse.class);
    private final ESTasks esTasks;
    private int nodesCount;

    public ElasticsearchNodesResponse(ESTasks esTasks, int nodesCount) {
        this.esTasks = esTasks;
        this.nodesCount = nodesCount;
        await().atMost(5, TimeUnit.MINUTES).pollInterval(1, TimeUnit.SECONDS).until(new ElasticsearchNodesCall());
    }

    private boolean discoverySuccessful = false;

    class ElasticsearchNodesCall implements Callable<Boolean> {

        // Returns whether to stop polling
        // Queries the Elasticsearch nodes list https://www.elastic.co/guide/en/elasticsearch/reference/1.6/cluster-nodes-info.html
        // `discoverySuccessful` is set to `true` iff return value is `true`
        @Override
        public Boolean call() throws Exception {
            try {
                for (JSONObject task : esTasks.getTasks()) {
                    if (!endpointIsOk(task)) {
                        LOGGER.info("At least one endpoint is not yet OK; will try again.");
                        return false;
                    }
                }
                LOGGER.info("All Elasticsearch endpoints succeeded");
                discoverySuccessful = true;
                return true;
            } catch (UnirestException e) {
                LOGGER.debug("Unable to get tasks list.", e);
                return false;
            }
        }

        // Returns `true` iff endpoint is OK.
        // Returns `false` iff endpoint is definitely not OK or it is not yet determined and we should continue polling.
        private boolean endpointIsOk(JSONObject task) {
            String url = "http://" + task.getString("http_address") + "/_nodes";

            GetRequest request = Unirest.get(url);

            HttpResponse<String> response = null;
            try {
                response = request.asString();
            } catch (UnirestException e) {
                LOGGER.info("Polling Elasticsearch endpoint '" + url + "' threw exception: " + e.getMessage());
                return false;
            }
            // response != null

            if (200 <= response.getStatus() && response.getStatus() < 400) {
                JsonNode body = null;

                try {
                    body = new JsonNode(response.getBody());
                } catch (RuntimeException e) {
                    LOGGER.info("Polling Elasticsearch endpoint '" + url + "' returned bad response body: " + e.getMessage());
                    return false;
                }
                // body != null

                if (body.getObject().getJSONObject("nodes").length() != nodesCount) {
                    LOGGER.info("Polling Elasticsearch endpoint '" + url + "' returned wrong number of nodes (Expected " + nodesCount + " but got " + body.getObject().getJSONObject("nodes").length() + ")");
                    return false;
                } else {
                    LOGGER.info("Polling Elasticsearch endpoint '" + url + "' succeeded");
                    return true;
                }
            } else {
                LOGGER.info("Polling Elasticsearch endpoint '" + url + "' returned bad status: " + response.getStatus() + " " + response.getStatusText());
                return false;
            }
        }
    }

    public boolean isDiscoverySuccessful() {
        return discoverySuccessful;
    }
}
