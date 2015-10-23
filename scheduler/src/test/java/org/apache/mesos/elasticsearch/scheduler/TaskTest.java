package org.apache.mesos.elasticsearch.scheduler;

import com.google.protobuf.ByteString;
import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.common.Discovery;
import org.junit.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.ZonedDateTime;
import java.util.Properties;

import static org.junit.Assert.assertNotNull;

/**
 * Test for Task
 */
public class TaskTest {
    public static final Protos.TaskID TASK_ID = Protos.TaskID.newBuilder().setValue("TaskID").build();

    @Test
    public void canParseTask() throws Exception {
        final Protos.TaskInfo taskInfo = createTaskInfo(ZonedDateTime.now());

        final Protos.TaskStatus taskStatus = Protos.TaskStatus.newBuilder()
                .setTaskId(TASK_ID)
                .setState(Protos.TaskState.TASK_STAGING)
                .build();

        final Task task = Task.from(taskInfo, taskStatus);
        assertNotNull(task.getStartedAt());
    }

    private Protos.TaskInfo createTaskInfo(ZonedDateTime zonedDateTime) {
        Protos.DiscoveryInfo.Builder discovery = Protos.DiscoveryInfo.newBuilder()
                .setPorts(Protos.Ports.newBuilder()
                        .addPorts(Discovery.CLIENT_PORT_INDEX, Protos.Port.newBuilder().setNumber(9001).setName(Discovery.CLIENT_PORT_NAME))
                        .addPorts(Discovery.TRANSPORT_PORT_INDEX, Protos.Port.newBuilder().setNumber(9002).setName(Discovery.TRANSPORT_PORT_NAME)))
                .setVisibility(Protos.DiscoveryInfo.Visibility.EXTERNAL);

        return Protos.TaskInfo.newBuilder()
                .setName("Name")
                .setTaskId(TASK_ID)
                .setSlaveId(Protos.SlaveID.newBuilder().setValue("SlaveID").build())
                .setData(toData("hostname", "ip address", zonedDateTime))
                .setDiscovery(discovery)
                .build();
    }

    public ByteString toData(String hostname, String ipAddress, ZonedDateTime zonedDateTime) {
        Properties data = new Properties();
        data.put("hostname", hostname);
        data.put("ipAddress", ipAddress);
        data.put("startedAt", zonedDateTime.toString());

        StringWriter writer = new StringWriter();
        data.list(new PrintWriter(writer));
        return ByteString.copyFromUtf8(writer.getBuffer().toString());
    }

}