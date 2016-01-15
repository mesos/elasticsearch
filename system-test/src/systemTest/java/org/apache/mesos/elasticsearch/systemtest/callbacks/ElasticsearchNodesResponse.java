package org.apache.mesos.elasticsearch.systemtest.callbacks;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import org.apache.log4j.Logger;
import org.apache.mesos.elasticsearch.common.elasticsearch.ElasticsearchParser;
import org.apache.mesos.elasticsearch.systemtest.ESTasks;
import org.json.JSONObject;

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
                        LOGGER.debug("At least one endpoint is not yet OK; will try again.");
                        return false;
                    }
                }
                LOGGER.debug("All Elasticsearch endpoints succeeded");
                discoverySuccessful = true;
                return true;
            } catch (Exception e) { // Catch all to prevent system test failures due to momentary errors with JSON parsing.
                LOGGER.debug("Unable to get task list. Retrying.", e);
                return false;
            }
        }

        // Returns `true` iff endpoint is OK.
        // Returns `false` iff endpoint is definitely not OK or it is not yet determined and we should continue polling.
        private boolean endpointIsOk(JSONObject task) throws Exception {
            String url = "http://" + ElasticsearchParser.parseHttpAddress(task) + "/_nodes";
            HttpResponse<String> response = Unirest.get(url).asString();

            if (response.getStatus() < 200 || response.getStatus() >= 400) {
                LOGGER.debug("Polling Elasticsearch endpoint '" + url + "' returned bad status: " + response.getStatus() + " " + response.getStatusText());
                return false;
            }

            JsonNode body = new JsonNode(response.getBody());
            if (body.getObject().getJSONObject("nodes").length() != nodesCount) {
                LOGGER.debug("Polling Elasticsearch endpoint '" + url + "' returned wrong number of nodes (Expected " + nodesCount + " but got " + body.getObject().getJSONObject("nodes").length() + ")");
                return false;
            }

            LOGGER.debug("Polling Elasticsearch endpoint '" + url + "' succeeded");
            return true;
        }
    }

    public boolean isDiscoverySuccessful() {
        return discoverySuccessful;
    }
}
