package org.apache.mesos.elasticsearch.systemtest;

import com.github.dockerjava.api.command.CreateContainerCmd;
import com.jayway.awaitility.Awaitility;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.mesos.mini.container.AbstractContainer;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Test Search Proxy controller
 */
public class SearchProxySystemTest extends TestBase {

    private static AbstractContainer dataImporter;

    private static String searchEndpoint;
    private static List<String> slavesElasticAddresses;

    @Rule
    public TestWatcher pusherWatch = new TestWatcher() { };

    @BeforeClass
    public static void importData() throws Exception {
        slavesElasticAddresses = new TasksResponse(getScheduler().getIpAddress(), CLUSTER.getConfig().getNumberOfSlaves()).getTasks().stream().map(jsonObject -> jsonObject.getString("http_address")).collect(toList());

        dataImporter = new AbstractContainer(CLUSTER.getConfig().dockerClient) {
            private String imageName;

            @Override
            protected void pullImage() {
                imageName = "mwldk/shakespeare-import";
                pullImage(imageName, "latest");
            }

            @Override
            protected CreateContainerCmd dockerCommand() {
                return dockerClient.createContainerCmd(imageName).withEnv("ELASTIC_SEARCH_URL=" + "http://" + slavesElasticAddresses.get(0));
            }
        };


        Awaitility.await().atMost(5, TimeUnit.MINUTES).pollDelay(2, TimeUnit.SECONDS).until(() -> {
            try {
                return Unirest.get("http://" + slavesElasticAddresses.get(0) + "/_nodes").asJson().getBody().getObject().getJSONObject("nodes").length() == 3;
            } catch (UnirestException e) {
                return false;
            }
        });
        final String importerId = CLUSTER.addAndStartContainer(dataImporter);
        Awaitility.await().atMost(2, TimeUnit.MINUTES).pollDelay(1, TimeUnit.SECONDS).until(() -> !CLUSTER.getConfig().dockerClient.inspectContainerCmd(importerId).exec().getState().isRunning());

        searchEndpoint = "http://" + getScheduler().getIpAddress() + ":31100/v1/es/_search";
    }

    @Test
    public void canRetrieveSearchResultFromRandomNode() throws Exception {
        final HttpResponse<JsonNode> response = Unirest.get(searchEndpoint).queryString("q", "love").asJson();
        assertEquals(200, response.getStatus());
        assertFalse(response.getBody().getObject().getBoolean("timed_out"));
        assertEquals(10, response.getBody().getObject().getJSONObject("hits").getJSONArray("hits").length());
    }

    @Test
    public void canRetrieveSearchResultFromParticularNode() throws Exception {
        final HttpResponse<JsonNode> response = Unirest.get(searchEndpoint).queryString("q", "love").header("X-ElasticSearch-Host", slavesElasticAddresses.get(1)).asJson();
        assertEquals(200, response.getStatus());
        assertFalse(response.getBody().getObject().getBoolean("timed_out"));
        assertEquals(10, response.getBody().getObject().getJSONObject("hits").getJSONArray("hits").length());
    }
}
