package org.apache.mesos.elasticsearch.systemtest;

import com.containersol.minimesos.config.ContainerConfigBlock;
import com.containersol.minimesos.container.AbstractContainer;
import com.containersol.minimesos.docker.DockerClientFactory;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;

import java.security.SecureRandom;

/**
 */
public class SeedDataContainer extends AbstractContainer {

    private static final String SEED_DATA_IMAGE = "mwldk/shakespeare-import";

    private String elasticSearchUrl;

    protected SeedDataContainer(DockerClient dockerClient, String elasticSearchUrl) {
        super(new ContainerConfigBlock(SEED_DATA_IMAGE, "latest"));
        this.elasticSearchUrl = elasticSearchUrl;
    }

    @Override
    public void pullImage() {
        pullImage(SEED_DATA_IMAGE, "latest");
    }

    @Override
    protected CreateContainerCmd dockerCommand() {
        return DockerClientFactory.build()
                .createContainerCmd(SEED_DATA_IMAGE)
                .withEnv("ELASTIC_SEARCH_URL=" + elasticSearchUrl)
                .withName("seed_data_" + new SecureRandom().nextInt());
    }

    @Override
    public String getRole() {
        return "seed_data";
    }

}
