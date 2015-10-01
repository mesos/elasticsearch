package org.apache.mesos.elasticsearch.scheduler.state;

import org.apache.log4j.Logger;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;

import java.io.IOException;
import java.security.InvalidParameterException;

/**
 * Status of task. This is necessary because the raw TaskInfo packet doesn't contain the frameworkID or a link to
 * the respective TaskStatus packet.
 */
public class ESTaskStatus {
    private static final Logger LOGGER = Logger.getLogger(TaskStatus.class);
    public static final String STATE_KEY = "state";
    public static final String DEFAULT_STATUS_NO_MESSAGE_SET = "Default status. No message set.";
    private final SerializableState state;
    private final FrameworkID frameworkID;

    private final TaskInfo taskInfo;

    private final StatePath statePath;
    public ESTaskStatus(SerializableState state, FrameworkID frameworkID, TaskInfo taskInfo, StatePath statePath) {
        if (state == null || taskInfo == null) {
            throw new InvalidParameterException("Cannot be null");
        } else if (frameworkID == null || frameworkID.getValue().isEmpty()) {
            throw new InvalidParameterException("FrameworkID cannot be null or empty");
        }
        this.state = state;
        this.frameworkID = frameworkID;
        this.taskInfo = taskInfo;
        this.statePath = statePath;
        // Write default status if it doesn't exist
        try {
            LOGGER.debug("Task status for " + taskInfo.getTaskId().getValue() + " exists, using old state: " + getStatus().getState());
        } catch (IllegalStateException | NullPointerException e) {
            LOGGER.debug("Task status for " + taskInfo.getTaskId().getValue() + " does not exist, or is not yet initialized, this must be a new task. Writing status.");
            setStatus(getDefaultStatus());
        }
    }

    public void setStatus(TaskStatus status) throws IllegalStateException {
        try {
            LOGGER.debug("Writing task status to zk: [" + status.getState() + "] " + status.getTaskId().getValue());
            statePath.mkdir(getKey());
            state.set(getKey(), status);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to write task status to zookeeper", e);
        }
    }

    public TaskStatus getStatus() throws IllegalStateException {
        try {
            return state.get(getKey());
        } catch (IOException e) {
            throw new IllegalStateException("Unable to get task status from zookeeper", e);
        }
    }

    public TaskStatus getDefaultStatus() {
        return TaskStatus.newBuilder()
                    .setState(TaskState.TASK_STAGING)
                    .setTaskId(taskInfo.getTaskId())
                    .setExecutorId(taskInfo.getExecutor().getExecutorId())
                    .setMessage(DEFAULT_STATUS_NO_MESSAGE_SET)
                    .build();
    }

    public TaskInfo getTaskInfo() {
        return taskInfo;
    }

    @Override
    public String toString() {
        String retVal;
        try {
            retVal = getKey() + ": [" + getStatus().getState() + "] " +  getStatus().getMessage();
        } catch (Exception e) {
            retVal = getKey() + ": Unable to get message";
        }
        return retVal;
    }

    private String getKey() {
        return frameworkID.getValue() + "/" + STATE_KEY + "/" + taskInfo.getTaskId().getValue();
    }

    public boolean taskInError() {
        TaskState state = getStatus().getState();
        return ESTaskStatus.errorState(state);
    }

    public static boolean errorState(TaskState state) {
        return state.equals(TaskState.TASK_ERROR) || state.equals(TaskState.TASK_FAILED)
                || state.equals(TaskState.TASK_LOST) || state.equals(TaskState.TASK_FINISHED);
    }

    public void destroy() {
        try {
            state.delete(getKey());
        } catch (IOException e) {
            LOGGER.error("Could not destroy Task in ZK.", e);
        }
    }
}
