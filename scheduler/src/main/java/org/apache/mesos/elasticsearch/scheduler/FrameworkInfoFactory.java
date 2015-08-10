package org.apache.mesos.elasticsearch.scheduler;

import org.apache.log4j.Logger;
import org.apache.mesos.Protos;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Factory for creating {@link org.apache.mesos.Protos.FrameworkInfo}s
 */
public class FrameworkInfoFactory {
    private static final Logger LOGGER = Logger.getLogger(FrameworkInfoFactory.class);

    private final Configuration configuration;

    public FrameworkInfoFactory(Configuration configuration) {
        this.configuration = configuration;
    }

    public Protos.FrameworkInfo.Builder getBuilder() {
        final Protos.FrameworkInfo.Builder frameworkBuilder = Protos.FrameworkInfo.newBuilder();
        frameworkBuilder.setUser("");
        frameworkBuilder.setName(configuration.getFrameworkName());
        frameworkBuilder.setFailoverTimeout(configuration.getFailoverTimeout());
        frameworkBuilder.setCheckpoint(true); // DCOS certification 04 - Checkpointing is enabled.
        setWebuiUrl(frameworkBuilder);
        setFrameworkId(frameworkBuilder);
        return frameworkBuilder;
    }

    private void setFrameworkId( Protos.FrameworkInfo.Builder frameworkBuilder) {
        Protos.FrameworkID frameworkID = configuration.getFrameworkId(); // DCOS certification 02
        if (frameworkID != null && !frameworkID.getValue().isEmpty()) {
            LOGGER.info("Found previous frameworkID: " + frameworkID);
            frameworkBuilder.setId(frameworkID);
        }
    }

    private void setWebuiUrl(Protos.FrameworkInfo.Builder frameworkBuilder) {
        try {
            String hostName = "http://" + InetAddress.getLocalHost().getHostName() + ":" + configuration.getManagementApiPort();
            LOGGER.debug("Setting webuiUrl to " + hostName);
            frameworkBuilder.setWebuiUrl(hostName);
        } catch (UnknownHostException e) {
            LOGGER.error("Unable to get hostname for webuiUrl");
        }
    }
}
