package org.apache.mesos.elasticsearch.systemtest;

import org.apache.log4j.Logger;
import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.common.zookeeper.formatter.MesosStateZKFormatter;
import org.apache.mesos.elasticsearch.common.zookeeper.formatter.ZKFormatter;
import org.apache.mesos.elasticsearch.common.zookeeper.parser.ZKAddressParser;
import org.apache.mesos.elasticsearch.scheduler.state.ClusterState;
import org.apache.mesos.elasticsearch.scheduler.state.FrameworkState;
import org.apache.mesos.elasticsearch.scheduler.state.SerializableState;
import org.apache.mesos.elasticsearch.scheduler.state.SerializableZookeeperState;
import org.apache.mesos.mini.MesosCluster;
import org.apache.mesos.mini.mesos.MesosClusterConfig;
import org.apache.mesos.state.ZooKeeperState;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.io.NotSerializableException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * Tests CLUSTER state mechanism
 */
public class ClusterStateTest {
    private static final Logger LOGGER = Logger.getLogger(ClusterStateTest.class);

    protected static final MesosClusterConfig CONFIG = MesosClusterConfig.builder()
            .numberOfSlaves(3)
            .privateRegistryPort(15000) // Currently you have to choose an available port by yourself
            .slaveResources(new String[]{"ports(*):[9200-9200,9300-9300]", "ports(*):[9201-9201,9301-9301]", "ports(*):[9202-9202,9302-9302]"})
            .build();
    public static final long ZK_TIMEOUT = 20000L;
    public static final String CLUSTER_NAME = "/mesos-ha";
    public static final String FRAMEWORK_NAME = "/elasticsearch-mesos";

    @ClassRule
    public static final MesosCluster CLUSTER = new MesosCluster(CONFIG);
    public static final int ZOOKEEPER_PORT = 2181;


    // ********** Development tests on local CLUSTER ***********
    /**
     * To run the following tests you need to have a native mesos library installed and route to the mesos local. E.g. for mac.
     * $ brew install mesos
     * $ sudo route -n add 172.17.0.0/16 $(docker-machine ip dev)
     */
    @Ignore
    @Test
    public void localClusterStateTest() throws NotSerializableException {
        String zkUrl = "zk://" + CLUSTER.getMesosContainer().getIpAddress() + ":" + ZOOKEEPER_PORT;
        SerializableState zkState = getState(getMesosStateZKURL(zkUrl));
        Protos.FrameworkID frameworkID = Protos.FrameworkID.newBuilder().setValue("frameworkId").build();
        FrameworkState frameworkState = new FrameworkState(zkState);
        frameworkState.setFrameworkId(frameworkID);
        ClusterState clusterState = new ClusterState(zkState, frameworkState);
        LOGGER.info("Setting slave list");
        clusterState.addTask(getNewTaskInfo(1));
        clusterState.addTask(getNewTaskInfo(2));
        clusterState.addTask(getNewTaskInfo(3));
        LOGGER.info("Set slave list");
        List<Protos.TaskInfo> executorStateList = clusterState.getTaskList();
        executorStateList.forEach(LOGGER::info);
        assertEquals(3, executorStateList.size());
        clusterState.removeTask(clusterState.getTask(Protos.TaskID.newBuilder().setValue("task1").build()));
        executorStateList = clusterState.getTaskList();
        executorStateList.forEach(LOGGER::info);
        assertEquals(2, executorStateList.size());
    }

    private Protos.TaskInfo getNewTaskInfo(int number) {
        Protos.SlaveID slaveID = Protos.SlaveID.newBuilder().setValue("slave" + number).build();
        Protos.ExecutorID executorID = Protos.ExecutorID.newBuilder().setValue("executor" + number).build();
        Protos.TaskID taskID = Protos.TaskID.newBuilder().setValue("task" + number).build();
        return Protos.TaskInfo.newBuilder().setTaskId(taskID).setExecutor(Protos.ExecutorInfo.newBuilder().setExecutorId(executorID)).setSlaveId(slaveID).build();
    }

    private String getMesosStateZKURL(String zkUrl) {
        ZKFormatter mesosStateZKFormatter = new MesosStateZKFormatter(new ZKAddressParser());
        return mesosStateZKFormatter.format(zkUrl);
    }

    private SerializableState getState(String zkUrl) {
        org.apache.mesos.state.State state = new ZooKeeperState(
                getMesosStateZKURL(zkUrl),
                ZK_TIMEOUT,
                TimeUnit.MILLISECONDS,
                FRAMEWORK_NAME + CLUSTER_NAME);
        return  new SerializableZookeeperState(state);
    }
}
