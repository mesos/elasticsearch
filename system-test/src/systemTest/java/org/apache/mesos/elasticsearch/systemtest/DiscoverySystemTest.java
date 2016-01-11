package org.apache.mesos.elasticsearch.systemtest;

import com.jayway.awaitility.Awaitility;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.mesos.elasticsearch.systemtest.base.SchedulerTestBase;
import org.json.JSONObject;
import org.junit.Test;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.TimeUnit;

/**
 * Given the wrong ES settings, the cluster will not discover each other. This test ensures it can.
 */
public class DiscoverySystemTest extends SchedulerTestBase {

    @Test
    public void shouldDiscoverInDockerMode() {
        ESTasks esTasks = new ESTasks(TEST_CONFIG, getScheduler().getIpAddress(), false);
        Awaitility.await().pollInterval(1L, TimeUnit.SECONDS).atMost(2L, TimeUnit.MINUTES).until(() -> clusterIsGreen(esTasks));
        Awaitility.await().pollInterval(1L, TimeUnit.SECONDS).atMost(2L, TimeUnit.MINUTES).until(() -> thereAreThreeNodes(esTasks));
    }

    private Boolean clusterIsGreen(ESTasks esTasks) {
        try {
            URL url = clusterHealthUrl(esTasks);

            String status = getJsonObjectFrom(url).getString("status");
            return "green".equals(status);
        } catch (MalformedURLException | UnirestException e) {
            return false;
        }
    }

    private Boolean thereAreThreeNodes(ESTasks esTasks) {
        try {
            URL url = clusterHealthUrl(esTasks);

            int numberOfNodes = getJsonObjectFrom(url).getInt("number_of_nodes");
            return TEST_CONFIG.getElasticsearchNodesCount() == numberOfNodes;
        } catch (MalformedURLException | UnirestException e) {
            return false;
        }
    }

    private URL clusterHealthUrl(ESTasks esTasks) throws UnirestException, MalformedURLException {
        JSONObject task = esTasks.getTasks().get(0);
        return new URL("http://" + task.getString("http_address") + "/_cluster/health");
    }

    private JSONObject getJsonObjectFrom(URL url) throws UnirestException {
        return Unirest.get(url.toString()).asJson().getBody().getObject();
    }
}
