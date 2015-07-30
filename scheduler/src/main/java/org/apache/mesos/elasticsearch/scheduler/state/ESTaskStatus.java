package org.apache.mesos.elasticsearch.scheduler.state;

import org.apache.log4j.Logger;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.elasticsearch.scheduler.State;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * Status of task
 */
public class ESTaskStatus {
    private static final Logger LOGGER = Logger.getLogger(TaskStatus.class);
    public static final String STATE = "state";
    private final State state;
    private final FrameworkID frameworkID;
    private final TaskInfo taskInfo;

    public ESTaskStatus(State state, FrameworkID frameworkID, TaskInfo taskInfo) {
        this.state = state;
        this.frameworkID = frameworkID;
        this.taskInfo = taskInfo;
    }

    public void setStatus(TaskStatus status) throws InterruptedException, ExecutionException, IOException {
        state.setAndCreateParents(getKey(), status);
    }

    public TaskStatus getStatus() throws InterruptedException, ExecutionException, ClassNotFoundException, IOException {
        return state.get(getKey());
    }

    public void setDefaultState() {
        try {
            setStatus(TaskStatus.newBuilder()
                    .setState(TaskState.TASK_STARTING)
                    .setTaskId(taskInfo.getTaskId())
                    .setExecutorId(taskInfo.getExecutor().getExecutorId())
                    .build());
        } catch (Exception e) {
            LOGGER.error("Unable to set default task state.");
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
