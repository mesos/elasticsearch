package org.apache.mesos.elasticsearch.scheduler.state;

import org.apache.log4j.Logger;
import org.apache.mesos.Protos;

import java.io.NotSerializableException;

/**
 * Model of framework state
 */
public class FrameworkState {
    private static final Logger LOGGER = Logger.getLogger(FrameworkState.class);
    private static final String FRAMEWORKID_KEY = "frameworkId";
    public static final Protos.FrameworkID EMPTY_ID = Protos.FrameworkID.newBuilder().setValue("").build();

    private final SerializableState state;
    private final StatePath statePath;

    public FrameworkState(SerializableState state) {
        this.state = state;
        statePath = new StatePath(state);
    }

    /**
     * Return empty if no frameworkId found.
     */
    public Protos.FrameworkID getFrameworkID() {
        Protos.FrameworkID id = null;
        try {
            id = state.get(FRAMEWORKID_KEY);
        } catch (NotSerializableException e) {
            LOGGER.warn("Unable to get FrameworkID from zookeeper", e);
        }
        return id == null ? EMPTY_ID : id;
    }

    public void setFrameworkId(Protos.FrameworkID frameworkId) {
        try {
            statePath.setAndCreateParents(FRAMEWORKID_KEY, frameworkId);
        } catch (NotSerializableException e) {
            LOGGER.error("Unable to store framework ID in zookeeper", e);
        }
    }
}