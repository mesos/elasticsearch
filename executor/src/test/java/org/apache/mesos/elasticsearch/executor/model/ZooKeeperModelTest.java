package org.apache.mesos.elasticsearch.executor.model;

import org.apache.mesos.Protos;
import org.junit.Test;

import java.security.InvalidParameterException;

import static org.junit.Assert.assertEquals;

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

    @Test(expected = InvalidParameterException.class)
    public void shouldExceptionIfIPFormat() {
        Protos.ExecutorInfo.Builder executorInfo = getDefaultExecutorInfo(Protos.CommandInfo.newBuilder().addArguments("-zk").addArguments("192.168.0.1"));
        Protos.TaskInfo.Builder taskInfo = getDefaultTaskInfo(executorInfo);
        new ZooKeeperModel(taskInfo.build());
    }

    @Test(expected = InvalidParameterException.class)
    public void shouldExceptionIfHTTPFormat() {
        Protos.ExecutorInfo.Builder executorInfo = getDefaultExecutorInfo(Protos.CommandInfo.newBuilder().addArguments("-zk").addArguments("http://192.168.0.1"));
        Protos.TaskInfo.Builder taskInfo = getDefaultTaskInfo(executorInfo);
        new ZooKeeperModel(taskInfo.build());
    }

    @Test
    public void shouldParseSingleZookeeperAddressCorrectly() {
        Protos.ExecutorInfo.Builder executorInfo = getDefaultExecutorInfo(Protos.CommandInfo.newBuilder().addArguments("-zk").addArguments("zk://192.168.0.1:2182"));
        Protos.TaskInfo.Builder taskInfo = getDefaultTaskInfo(executorInfo);
        ZooKeeperModel zooKeeperModel = new ZooKeeperModel(taskInfo.build());
        assertEquals("zk://192.168.0.1:2182", zooKeeperModel.getRuntimeSettings().get(ZooKeeperModel.ZOOKEEPER_ADDRESS_KEY));
    }

    @Test
    public void shouldParseMultiZookeeperAddressCorrectly() {
        Protos.ExecutorInfo.Builder executorInfo = getDefaultExecutorInfo(Protos.CommandInfo.newBuilder().addArguments("-zk").addArguments("zk://192.168.0.1:2182,10.4.52.3:1234/mesos"));
        Protos.TaskInfo.Builder taskInfo = getDefaultTaskInfo(executorInfo);
        ZooKeeperModel zooKeeperModel = new ZooKeeperModel(taskInfo.build());
        assertEquals("zk://192.168.0.1:2182,10.4.52.3:1234/mesos", zooKeeperModel.getRuntimeSettings().get(ZooKeeperModel.ZOOKEEPER_ADDRESS_KEY));
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