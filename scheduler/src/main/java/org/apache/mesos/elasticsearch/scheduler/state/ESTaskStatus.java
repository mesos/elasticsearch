package org.apache.mesos.elasticsearch.scheduler.state;

import org.apache.log4j.Logger;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;

import java.io.IOException;
import java.io.NotSerializableException;
import java.security.InvalidParameterException;
import java.util.concurrent.ExecutionException;

/**
 * Status of task
 */
public class ESTaskStatus {
    private static final Logger LOGGER = Logger.getLogger(TaskStatus.class);
    public static final String STATE = "state";
    private final SerializableState state;
    private final FrameworkID frameworkID;
    private final TaskInfo taskInfo;
    private final org.apache.mesos.elasticsearch.scheduler.state.State stateHelp;

    public ESTaskStatus(SerializableState state, FrameworkID frameworkID, TaskInfo taskInfo) {
        if (state == null) {
            throw new InvalidParameterException("State cannot be null");
        } else if (frameworkID == null || frameworkID.getValue().isEmpty()) {
            throw new InvalidParameterException("FrameworkID cannot be null or empty");
        }
        this.state = state;
        this.frameworkID = frameworkID;
        this.taskInfo = taskInfo;
        stateHelp = new State(state);
    }

    public void setStatus(TaskStatus status) throws InterruptedException, ExecutionException, IOException, ClassNotFoundException {
        LOGGER.debug("Writing task status to zk: [" + status.getTimestamp() + "] " + status.getTaskId().getValue());
        stateHelp.setAndCreateParents(getKey(), status);
    }

    public TaskStatus getStatus() throws IllegalStateException {
        try {
            return state.get(getKey());
        } catch (NotSerializableException e) {
            throw new IllegalStateException("Unable to get task status from zookeeper", e);
        }
    }

    public void setDefaultState() {
        try {
            setStatus(TaskStatus.newBuilder()
                    .setState(TaskState.TASK_STARTING)
                    .setTaskId(taskInfo.getTaskId())
                    .setExecutorId(taskInfo.getExecutor().getExecutorId())
                    .build());
        } catch (InterruptedException | ExecutionException | IOException | ClassNotFoundException e) {
            LOGGER.error("Unable to set default task state.", e);
        }
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

    private String getKey() {
        return frameworkID.getValue() + "/" + STATE + "/" + taskInfo.getTaskId().getValue();
    }

}
