package org.apache.mesos.elasticsearch.scheduler.state;

import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.elasticsearch.scheduler.State;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * Model for writing executor state to zookeeper
 */
public class ExecutorState {
    public static final String STATE = "state";
    private final FrameworkID frameworkID;
    private final SlaveID slaveID;
    private final State state;

    public ExecutorState(State state, FrameworkID frameworkID, SlaveID slaveID) {
        this.state = state;
        this.frameworkID = frameworkID;
        this.slaveID = slaveID;
    }

    public void setStatus(TaskStatus status) throws InterruptedException, ExecutionException, IOException {
        state.set(getKey(), status);
    }

    public TaskStatus getStatus() throws InterruptedException, ExecutionException, ClassNotFoundException, IOException {
        return state.get(getKey());
    }

    private String getKey() {
        return frameworkID.getValue() + "/" + STATE + "/" + slaveID.getValue();
    }

    @Override
    public String toString() {
        String retVal;
        try {
            retVal = getKey() + ": " + getStatus().getMessage();
        } catch (Exception e) {
            retVal = getKey() + ": Unable to get message";
        }
        return retVal;
    }
}
