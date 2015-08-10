package org.apache.mesos.elasticsearch.scheduler.util;

import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.common.Discovery;

import java.util.UUID;

/**
 * Proto file helpers for tests.
 */
public class ProtoTestUtil {
    public static Protos.TaskInfo getDefaultTaskInfo() {
        return Protos.TaskInfo.newBuilder()
                .setName("dummyTaskName")
                .setTaskId(Protos.TaskID.newBuilder().setValue(UUID.randomUUID().toString()))
                .setSlaveId(Protos.SlaveID.newBuilder().setValue(UUID.randomUUID().toString()).build())
                .setExecutor(Protos.ExecutorInfo.newBuilder()
                        .setExecutorId(Protos.ExecutorID.newBuilder().setValue("executorID").build())
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
}
