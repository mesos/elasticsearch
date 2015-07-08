package org.apache.mesos.elasticsearch.systemtest;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.DockerClientConfig;
import com.mashape.unirest.http.Unirest;
import org.apache.http.HttpHost;
import org.apache.log4j.Logger;
import org.junit.Before;

/**
 * Base test class for everything shared between test cases
 */
public abstract class TestBase {
    private static final Logger LOGGER = Logger.getLogger(TestBase.class);
    DockerClient docker;

    protected String getSlaveIp(String slaveName) {

        InspectContainerResponse response = docker.inspectContainerCmd(slaveName).exec();

        return response.getNetworkSettings().getIpAddress();
    }

    @Before
    public void setUp() throws Exception {
        DockerClientConfig config = DockerClientConfig.createDefaultConfigBuilder()
                .withVersion("1.16")
                .build();

        docker = DockerClientBuilder.getInstance(config).build();
        HttpHost proxy = new HttpHost(config.getUri().getHost(), 8888);
        Unirest.setProxy(proxy);
        LOGGER.info("Using proxy " + proxy.toHostString());
        System.out.println("proxy.toHostString() = " + proxy.toHostString());
        Unirest.setDefaultHeader("Content-Type", "application/json");
    }
}
