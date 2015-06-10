package org.apache.mesos.elasticsearch.systemtest;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.DockerClientConfig;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.json.JSONObject;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static com.jayway.awaitility.Awaitility.await;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests REST node discovery
 */
public class DiscoverySystemTest {

    @Test
    public void testNodeDiscoveryRest() throws InterruptedException {
        DockerClientConfig config = DockerClientConfig.createDefaultConfigBuilder()
                .withVersion("1.16")
                .withUri("unix:///var/run/docker.sock")
                .build();

        DockerClient docker = DockerClientBuilder.getInstance(config).build();

        InspectContainerResponse response = docker.inspectContainerCmd("mesoses_slave1_1").exec();

        String ipAddress = response.getNetworkSettings().getIpAddress();

        ElasticsearchNodesResponse nodesResponse = new ElasticsearchNodesResponse(ipAddress);
        JSONObject jsonNodesResponse = await().atMost(5, TimeUnit.MINUTES).pollInterval(1, TimeUnit.SECONDS).until(nodesResponse, notNullValue());

        assertEquals(3, jsonNodesResponse.getJSONObject("nodes").length());

        fail("Connection to Elasticsearch timed out");
    }

    private static class ElasticsearchNodesResponse implements Callable<JSONObject> {

        private String ipAddress;

        public ElasticsearchNodesResponse(String ipAddress) {
            this.ipAddress = ipAddress;
        }

        @Override
        public JSONObject call() throws Exception {
            try {
                JSONObject object = Unirest.get("http://" + ipAddress + ":9200/_nodes").asJson().getBody().getObject();
                System.out.println("Retrieving node response");
                return object;
            } catch (UnirestException e) {
                System.out.println("Elasticsearch does not yet listen on port 9200");
                return null;
            }
        }
    }

}
