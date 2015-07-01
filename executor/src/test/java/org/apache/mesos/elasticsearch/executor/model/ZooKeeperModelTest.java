package org.apache.mesos.elasticsearch.executor.model;

import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.common.zookeeper.exception.ZKAddressException;
import org.junit.Test;

import java.security.InvalidParameterException;

import static org.junit.Assert.assertEquals;

/**
 * Tests
 */
@SuppressWarnings({"PMD.AvoidUsingHardCodedIP"})
public class ZooKeeperModelTest {
    @Test(expected = NullPointerException.class)
    public void shouldExceptionIfPassedNull() {
        new ZooKeeperModel(null);
    }

    @Test(expected = InvalidParameterException.class)
    public void shouldExceptionIfNoZKInfo() {
        Protos.TaskInfo taskInfo = Protos.TaskInfo.getDefaultInstance();
        new ZooKeeperModel(taskInfo);
    }

    @Test(expected = InvalidParameterException.class)
    public void shouldExceptionIfOnlyParameterCommand() {
        Protos.ExecutorInfo.Builder executorInfo = getDefaultExecutorInfo(Protos.CommandInfo.newBuilder().addArguments("-zk"));
        Protos.TaskInfo.Builder taskInfo = getDefaultTaskInfo(executorInfo);
        new ZooKeeperModel(taskInfo.build());
    }

    private Protos.ExecutorInfo.Builder getDefaultExecutorInfo(Protos.CommandInfo.Builder commandInfo) {
        return Protos.ExecutorInfo.newBuilder()
                .setCommand(commandInfo)
                .setExecutorId(Protos.ExecutorID.newBuilder().setValue("0"));
    }

    private Protos.TaskInfo.Builder getDefaultTaskInfo(Protos.ExecutorInfo.Builder executorInfo) {
        return Protos.TaskInfo.newBuilder()
                .setName("")
                .setTaskId(Protos.TaskID.newBuilder().setValue("0"))
                .setSlaveId(Protos.SlaveID.newBuilder().setValue("0"))
                .setExecutor(executorInfo);
    }
}