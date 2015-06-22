package org.apache.mesos.elasticsearch.executor;

import org.apache.log4j.Logger;
import org.apache.mesos.Protos;

/**
 * Wraps the TaskState status updates into easy to read methods.
 */
public class TaskStatus {
    private static final Logger LOGGER = Logger.getLogger(TaskStatus.class.getCanonicalName());
    private final Protos.TaskID taskID;

    public TaskStatus(Protos.TaskID taskID) {
        this.taskID = taskID;
    }

    public Protos.TaskStatus getTaskStatus(Protos.TaskState taskState) {
        return Protos.TaskStatus.newBuilder()
                .setTaskId(taskID)
                .setState(taskState).build();
    }

    public Protos.TaskStatus running() {
        LOGGER.info("TASK_RUNNING");
        return getTaskStatus(Protos.TaskState.TASK_RUNNING);
    }

    public Protos.TaskStatus failed() {
        LOGGER.info("TASK_FAILED");
        return getTaskStatus(Protos.TaskState.TASK_FAILED);
    }

    public Protos.TaskStatus error() {
        LOGGER.info("TASK_ERROR");
        return getTaskStatus(Protos.TaskState.TASK_ERROR);
    }

    public Protos.TaskStatus starting() {
        LOGGER.info("TASK_STARTING");
        return getTaskStatus(Protos.TaskState.TASK_STARTING);
    }

    public Protos.TaskStatus finished() {
        LOGGER.info("TASK_FINISHED");
        return getTaskStatus(Protos.TaskState.TASK_FINISHED);
    }
}