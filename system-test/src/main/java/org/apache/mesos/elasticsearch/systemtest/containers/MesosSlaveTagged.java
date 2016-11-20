package org.apache.mesos.elasticsearch.systemtest.containers;

import com.containersol.minimesos.config.AgentResourcesConfig;
import com.containersol.minimesos.config.MesosAgentConfig;
import com.containersol.minimesos.mesos.MesosAgentContainer;
import org.apache.mesos.elasticsearch.systemtest.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A tagged version of MesosSlave
 */
public class MesosSlaveTagged extends MesosAgentContainer {

    private static final Logger LOG = LoggerFactory.getLogger(MesosSlaveTagged.class);

    public MesosSlaveTagged(String resources) {
        super(new MesosAgentConfig("1.0.0"));
        LOG.debug("Using image: " + MesosAgentConfig.MESOS_AGENT_IMAGE + ":" + Configuration.MESOS_IMAGE_TAG);
    }

//    private static MesosAgentConfig agentConfig(String resources, int port, String imageName, String imageTag) {
//
//        MesosAgentConfig config = new MesosAgentConfig();
//
//        AgentResourcesConfig resourcesConfig = AgentResourcesConfig.fromString(resources);
//        config.setResources(resourcesConfig);
//
//        config.setPortNumber(port);
//
//        config.setImageName(imageName);
//        config.setImageTag(imageTag);
//
//        return config;
//
//    }

}
