package org.apache.mesos.elasticsearch.scheduler.cluster;

import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.scheduler.Task;
import org.apache.mesos.elasticsearch.scheduler.TaskInfoFactory;
import org.apache.mesos.elasticsearch.scheduler.state.ClusterState;
import org.apache.mesos.elasticsearch.scheduler.util.Clock;

import java.util.HashMap;
import java.util.Map;

/**
 * Helper methods for cluster state
 */
public class ClusterStateUtil {
    public static final String DEFAULT_STATUS_NO_MESSAGE_SET = "Default status. No message set.";

    public static Protos.TaskStatus getDefaultTaskStatus(Protos.TaskInfo taskInfo) {
        return Protos.TaskStatus.newBuilder()
                .setState(Protos.TaskState.TASK_STAGING)
                .setTaskId(taskInfo.getTaskId())
                .setMessage(DEFAULT_STATUS_NO_MESSAGE_SET)
                .build();
    }

    /**
     * Get a list of all tasks in a format specific to the web GUI.
     *
     * @return
     */
    public static Map<String, Task> getGuiTaskList(ClusterState clusterState) {
        Map<String, Task> tasks = new HashMap<>();
        clusterState.get().forEach(task -> tasks.put(task.getTask().getTaskId().getValue(), TaskInfoFactory.parse(task.getTask(), task.getStatus(), new Clock())));
        return tasks;
    }

    public static ESTask getESTaskForStatus(ClusterState clusterState, Protos.TaskStatus taskStatus) {
        return clusterState.get().stream()
                .filter(task -> task.getTask().getTaskId().getValue().equals(taskStatus.getTaskId().getValue()))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Cannot find task with id " + taskStatus.getTaskId().getValue() + " in task list."));
    }

    public static ESTask getESTaskForExecutorId(ClusterState clusterState, Protos.ExecutorID executorID) {
        return clusterState.get().stream()
                .filter(task -> task.getTask().getExecutor().getExecutorId().equals(executorID))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Cannot find task with id " + executorID.getValue() + " in task list."));
    }

    public boolean taskInError(ESTask esTask) {
        Protos.TaskState state = esTask.getStatus().getState();
        return errorState(state);
    }

    private static boolean errorState(Protos.TaskState state) {
        return state.equals(Protos.TaskState.TASK_ERROR) || state.equals(Protos.TaskState.TASK_FAILED)
                || state.equals(Protos.TaskState.TASK_LOST) || state.equals(Protos.TaskState.TASK_FINISHED)
                || state.equals(Protos.TaskState.TASK_KILLED);
    }
}
