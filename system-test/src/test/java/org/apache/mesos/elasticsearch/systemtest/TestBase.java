package org.apache.mesos.elasticsearch.systemtest;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.DockerClientConfig;
import com.mashape.unirest.http.Unirest;
import org.apache.http.impl.client.HttpClients;
import org.junit.Before;

public abstract class TestBase {
    protected String getSlaveIp(String slaveName) {
        DockerClientConfig config = DockerClientConfig.createDefaultConfigBuilder()
                .withVersion("1.16")
                .build();

        DockerClient docker = DockerClientBuilder.getInstance(config).build();

        InspectContainerResponse response = docker.inspectContainerCmd(slaveName).exec();

        return response.getNetworkSettings().getIpAddress();
    }

    @Before
    public void setUp() throws Exception {
        //set up http proxy if http.proxyHost and http.proxyPort system properties are defined
        Unirest.setHttpClient(HttpClients.createSystem());
        Unirest.setDefaultHeader("Content-Type", "application/json");
    }
}
