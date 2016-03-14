package org.apache.mesos.elasticsearch.systemtest.containers;

import com.containersol.minimesos.config.AgentResourcesConfig;
import com.containersol.minimesos.config.MesosAgentConfig;
import com.containersol.minimesos.mesos.DockerClientFactory;
import com.containersol.minimesos.mesos.MesosAgent;
import com.containersol.minimesos.mesos.ZooKeeper;
import org.apache.mesos.elasticsearch.systemtest.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A tagged version of MesosSlave
 */
public class MesosSlaveTagged extends MesosAgent {
    private static final Logger LOG = LoggerFactory.getLogger(MesosSlaveTagged.class);
    public MesosSlaveTagged(ZooKeeper zooKeeperContainer, String resources) {
        super(DockerClientFactory.build(), zooKeeperContainer, agentConfig(resources, MesosAgentConfig.DEFAULT_MESOS_AGENT_PORT, MesosAgentConfig.MESOS_AGENT_IMAGE, Configuration.MESOS_IMAGE_TAG ));
        LOG.debug("Using image: " + MesosAgentConfig.MESOS_AGENT_IMAGE + ":" + Configuration.MESOS_IMAGE_TAG);
    }

    private static MesosAgentConfig agentConfig(String resources, int port, String imageName, String imageTag) {

        MesosAgentConfig config = new MesosAgentConfig();

        AgentResourcesConfig resourcesConfig = AgentResourcesConfig.fromString(resources);
        config.setResources(resourcesConfig);

        config.setPortNumber(port);

        config.setImageName(imageName);
        config.setImageTag(imageTag);

        return config;

    }

}
