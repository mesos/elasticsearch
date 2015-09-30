package org.apache.mesos.elasticsearch.scheduler.state;

import org.apache.log4j.Logger;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;

import java.io.IOException;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Model of framework state
 */
public class FrameworkState {
    private static final Logger LOGGER = Logger.getLogger(FrameworkState.class);
    private static final String FRAMEWORKID_KEY = "frameworkId";
    public static final Protos.FrameworkID EMPTY_ID = Protos.FrameworkID.newBuilder().setValue("").build();
    private List<Consumer<ClusterState>> registeredListeners = new Vector<>();
    private List<Consumer<ESTaskStatus>> newTaskListeners = new Vector<>();
    private AtomicBoolean registered = new AtomicBoolean(false);

    private final SerializableState state;
    private final StatePath statePath;
    private SchedulerDriver driver;

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
        } catch (IOException e) {
            LOGGER.warn("Unable to get FrameworkID from zookeeper", e);
        }
        return id == null ? EMPTY_ID : id;
    }

    public void markRegistered(Protos.FrameworkID frameworkId, SchedulerDriver driver) {
        if (!registered.compareAndSet(false, true)) {
            throw new IllegalStateException("Framework can not be marked as registered twice");
        }
        try {
            statePath.mkdir(FRAMEWORKID_KEY);
            state.set(FRAMEWORKID_KEY, frameworkId);
        } catch (IOException e) {
            LOGGER.error("Unable to store framework ID in zookeeper", e);
        }
        this.driver = driver;
        final ClusterState clusterState = new ClusterState(state, this);
        registeredListeners.forEach(listener -> listener.accept(clusterState));
    }

    public SchedulerDriver getDriver() {
        return driver;
    }

    public void onRegistered(Consumer<ClusterState> listener) {
        if (registered.get()) {
            throw new IllegalStateException("Framework has already been registered");
        }
        registeredListeners.add(listener);
    }

    public void onNewTask(Consumer<ESTaskStatus> listener) {
        newTaskListeners.add(listener);
    }

    public boolean isRegistered() {
        return registered.get();
    }

    public void announceNewTask(ESTaskStatus esTask) {
        newTaskListeners.forEach(newTaskStatusConsumer -> newTaskStatusConsumer.accept(esTask));
    }
}