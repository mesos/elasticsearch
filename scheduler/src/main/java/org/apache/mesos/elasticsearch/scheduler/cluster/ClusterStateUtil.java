package org.apache.mesos.elasticsearch.scheduler.cluster;

import org.apache.mesos.Protos;

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
}
