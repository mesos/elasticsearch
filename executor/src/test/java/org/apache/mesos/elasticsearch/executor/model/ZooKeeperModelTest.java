package org.apache.mesos.elasticsearch.executor.model;

import org.apache.mesos.Protos;
import org.junit.Test;

import java.security.InvalidParameterException;

import static org.junit.Assert.*;

/**
 * Tests
 */
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

    @Test(expected = InvalidParameterException.class)
    public void shouldExceptionIfOnlyParameterValue() {
        Protos.ExecutorInfo.Builder executorInfo = getDefaultExecutorInfo(Protos.CommandInfo.newBuilder().addArguments("ZK_ADDRESS"));
        Protos.TaskInfo.Builder taskInfo = getDefaultTaskInfo(executorInfo);
        new ZooKeeperModel(taskInfo.build());
    }

    @Test
    public void shouldParseZKInfoCorrectly() {
        Protos.ExecutorInfo.Builder executorInfo = getDefaultExecutorInfo(Protos.CommandInfo.newBuilder().addArguments("-zk").addArguments("ZK_ADDRESS"));
        Protos.TaskInfo.Builder taskInfo = getDefaultTaskInfo(executorInfo);
        ZooKeeperModel zooKeeperModel = new ZooKeeperModel(taskInfo.build());
        assertEquals("ZK_ADDRESS", zooKeeperModel.getAddress().get(ZooKeeperModel.ZOOKEEPER_ADDRESS_KEY));
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