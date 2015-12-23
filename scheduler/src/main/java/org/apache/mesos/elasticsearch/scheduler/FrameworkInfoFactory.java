package org.apache.mesos.elasticsearch.scheduler;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.scheduler.state.FrameworkState;

/**
 * Factory for creating {@link org.apache.mesos.Protos.FrameworkInfo}s
 */
public class FrameworkInfoFactory {
    private static final Logger LOGGER = Logger.getLogger(FrameworkInfoFactory.class);

    private final Configuration configuration;
    private FrameworkState frameworkState;

    public FrameworkInfoFactory(Configuration configuration, FrameworkState frameworkState) {
        this.configuration = configuration;
        this.frameworkState = frameworkState;
    }

    public Protos.FrameworkInfo.Builder getBuilder() {
        final Protos.FrameworkInfo.Builder frameworkBuilder = Protos.FrameworkInfo.newBuilder();
        frameworkBuilder.setUser("");
        frameworkBuilder.setName(configuration.getFrameworkName());
        frameworkBuilder.setFailoverTimeout(configuration.getFailoverTimeout());
        frameworkBuilder.setCheckpoint(true); // DCOS certification 04 - Checkpointing is enabled.
        frameworkBuilder.setRole(configuration.getFrameworkRole()); // DCOS certification requirement 13
        setWebuiUrl(frameworkBuilder);
        setFrameworkId(frameworkBuilder);
        setFrameworkPrincipal(frameworkBuilder);
        return frameworkBuilder;
    }

    private void setFrameworkPrincipal(Protos.FrameworkInfo.Builder frameworkBuilder) {
        String frameworkPrincipal = configuration.getFrameworkPrincipal();
        if (!StringUtils.isEmpty(frameworkPrincipal)) {
            LOGGER.debug("Using framework principal: " + frameworkPrincipal);
            frameworkBuilder.setPrincipal(frameworkPrincipal);
        }
    }

    private void setFrameworkId(Protos.FrameworkInfo.Builder frameworkBuilder) {
        Protos.FrameworkID frameworkID = frameworkState.getFrameworkID(); // DCOS certification 02
        if (frameworkID != null && !frameworkID.getValue().isEmpty()) {
            LOGGER.info("Found previous frameworkID: " + frameworkID);
            frameworkBuilder.setId(frameworkID);
        }
    }

    private void setWebuiUrl(Protos.FrameworkInfo.Builder frameworkBuilder) {
        String hostName = configuration.webUiAddress();
        LOGGER.debug("Setting webuiUrl to " + hostName);
        frameworkBuilder.setWebuiUrl(hostName);
    }
}
