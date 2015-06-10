package org.apache.mesos.elasticsearch.systemtest;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.DockerClientConfig;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.json.JSONObject;
import org.junit.Test;

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

        // TODO: Replace with awaitility
        long startTime = System.currentTimeMillis();
        JSONObject nodes;
        while (System.currentTimeMillis() - startTime < 60 * 1000) {
            try {
                nodes = Unirest.get("http://" + ipAddress + ":9200/_nodes").asJson().getBody().getObject();
            } catch (UnirestException e) {
                Thread.sleep(1000);
                continue;
            }
            assertEquals(3, nodes.getJSONObject("nodes").length());
            return;
        }
        fail("Connection to Elasticsearch timed out");
    }
}
