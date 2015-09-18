package org.apache.mesos.elasticsearch.scheduler.util;

import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.common.Discovery;

/**
 * Proto file helpers for tests.
 */
public class ProtoTestUtil {
    public static final String FRAMEWORK_ID = "frameworkId";
    public static final String EXECUTOR_ID = "executorId";
    public static final String TASK_ID = "task1";
    public static final String SLAVE_ID = "slaveID";

    public static Protos.TaskInfo getDefaultTaskInfo() {
        return Protos.TaskInfo.newBuilder()
                .setName("dummyTaskName")
                .setTaskId(Protos.TaskID.newBuilder().setValue(SLAVE_ID))
                .setSlaveId(Protos.SlaveID.newBuilder().setValue(SLAVE_ID).build())
                .setExecutor(Protos.ExecutorInfo.newBuilder()
                        .setExecutorId(Protos.ExecutorID.newBuilder().setValue(EXECUTOR_ID).build())
                        .setCommand(Protos.CommandInfo.newBuilder().setValue("").build())
                        .build())
                .setDiscovery(
                        Protos.DiscoveryInfo.newBuilder()
                                .setVisibility(Protos.DiscoveryInfo.Visibility.EXTERNAL)
                                .setPorts(Protos.Ports.newBuilder()
                                                .addPorts(Discovery.CLIENT_PORT_INDEX, Protos.Port.newBuilder().setNumber(9200))
                                                .addPorts(Discovery.TRANSPORT_PORT_INDEX, Protos.Port.newBuilder().setNumber(9300))
                                )
                )
                .build();
    }

    public static Protos.TaskStatus getDefaultTaskStatus(Protos.TaskState state) {
        Protos.SlaveID slaveID = Protos.SlaveID.newBuilder().setValue(SLAVE_ID).build();
        Protos.ExecutorID executorID = Protos.ExecutorID.newBuilder().setValue(EXECUTOR_ID).build();
        Protos.TaskID taskID = Protos.TaskID.newBuilder().setValue(TASK_ID).build();
        return Protos.TaskStatus.newBuilder().setSlaveId(slaveID).setTaskId(taskID).setExecutorId(executorID)
                .setState(state).build();
    }
}
