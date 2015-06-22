package org.apache.mesos.elasticsearch.executor;

import org.apache.mesos.Protos;

public class TaskStatus {
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
        return getTaskStatus(Protos.TaskState.TASK_RUNNING);
    }

    public Protos.TaskStatus failed() {
        return getTaskStatus(Protos.TaskState.TASK_FAILED);
    }

    public Protos.TaskStatus error() {
        return getTaskStatus(Protos.TaskState.TASK_ERROR);
    }

    public Protos.TaskStatus starting() {
        return getTaskStatus(Protos.TaskState.TASK_STARTING);
    }

    public Protos.TaskStatus finished() {
        return getTaskStatus(Protos.TaskState.TASK_FINISHED);
    }
}