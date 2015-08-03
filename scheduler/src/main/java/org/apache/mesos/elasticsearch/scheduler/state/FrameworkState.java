package org.apache.mesos.elasticsearch.scheduler.state;

import org.apache.log4j.Logger;
import org.apache.mesos.Protos;

import java.io.NotSerializableException;

public class FrameworkState {
    private static final Logger LOGGER = Logger.getLogger(FrameworkState.class);
    private static final String FRAMEWORKID_KEY = "frameworkId";

    private final State state;

    public FrameworkState(State state) {
        this.state = state;
    }

    /**
     * Return empty if no frameworkId found.
     */
    public Protos.FrameworkID getFrameworkID() {
        Protos.FrameworkID id = state.get(FRAMEWORKID_KEY);
        if (id == null) {
            id = Protos.FrameworkID.newBuilder().setValue("").build();
        }
        return id;
    }

    public void setFrameworkId(Protos.FrameworkID frameworkId) {
        try {
            state.setAndCreateParents(FRAMEWORKID_KEY, frameworkId);
        } catch (NotSerializableException e) {
            LOGGER.error("Unable to store framework ID in zookeeper", e);
        }
    }
}