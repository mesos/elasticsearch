package org.apache.mesos.elasticsearch.systemtest;

import org.apache.log4j.Logger;
import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.common.zookeeper.formatter.MesosStateZKFormatter;
import org.apache.mesos.elasticsearch.common.zookeeper.formatter.ZKFormatter;
import org.apache.mesos.elasticsearch.common.zookeeper.parser.ZKAddressParser;
import org.apache.mesos.elasticsearch.scheduler.State;
import org.apache.mesos.elasticsearch.scheduler.ZooKeeperStateInterfaceImpl;
import org.apache.mesos.elasticsearch.scheduler.state.ClusterState;
import org.apache.mesos.elasticsearch.scheduler.state.ExecutorState;
import org.apache.mesos.mini.MesosCluster;
import org.apache.mesos.mini.mesos.MesosClusterConfig;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Tests cluster state mechanism
 */
public class ClusterStateTest {
    private static final Logger LOGGER = Logger.getLogger(ClusterStateTest.class);

    protected static final MesosClusterConfig config = MesosClusterConfig.builder()
            .numberOfSlaves(3)
            .privateRegistryPort(15000) // Currently you have to choose an available port by yourself
            .slaveResources(new String[]{"ports(*):[9200-9200,9300-9300]", "ports(*):[9201-9201,9301-9301]", "ports(*):[9202-9202,9302-9302]"})
            .build();

    @ClassRule
    public static final MesosCluster cluster = new MesosCluster(config);


    /**
     * To run the following tests you need to have a native mesos library installed and route to the mesos local. E.g. for mac.
     * $ brew install mesos
     * $ sudo route -n add 172.17.0.0/16 $(docker-machine ip dev)
     */
    @Ignore
    @Test
    public void localClusterStateTest() {
        String zkUrl = "zk://" + cluster.getMesosContainer().getIpAddress() + ":2181";
        ZooKeeperStateInterfaceImpl zkState = new ZooKeeperStateInterfaceImpl(getMesosStateZKURL(zkUrl));
        State state = new State(zkState);
        state.setFrameworkId(Protos.FrameworkID.newBuilder().setValue("exampleId").build());
        ClusterState clusterState = new ClusterState(state);
        LOGGER.info("Setting slave list");
        clusterState.addSlave(Protos.SlaveID.newBuilder().setValue("slave1").build());
        clusterState.addSlave(Protos.SlaveID.newBuilder().setValue("slave2").build());
        clusterState.addSlave(Protos.SlaveID.newBuilder().setValue("slave3").build());
        LOGGER.info("Set slave list");
        List<ExecutorState> executorStateList = clusterState.getState();
        executorStateList.forEach(LOGGER::info);
        assertEquals(3, executorStateList.size());
    }

    private String getMesosStateZKURL(String zkUrl) {
        ZKFormatter mesosStateZKFormatter = new MesosStateZKFormatter(new ZKAddressParser());
        return mesosStateZKFormatter.format(zkUrl);
    }
}
