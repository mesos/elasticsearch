package org.apache.mesos.elasticsearch.executor.model;

import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.common.Discovery;
import org.junit.Test;

import java.security.InvalidParameterException;

import static org.junit.Assert.*;

/**
 * Tests
 */
public class PortsModelTest {
    @Test(expected = NullPointerException.class)
    public void shouldExceptionIfPassedNull() {
        new PortsModel(null);
    }

    @Test(expected = InvalidParameterException.class)
    public void shouldExceptionIfNoDiscoveryInfo() {
        Protos.TaskInfo taskInfo = Protos.TaskInfo.getDefaultInstance();
        new PortsModel(taskInfo);
    }

    @Test(expected = InvalidParameterException.class)
    public void shouldExceptionIfIncorrectNumberOfPorts() {
        Protos.TaskInfo taskInfo = getDefaultTaskInfo(getDefaultDiscoveryInfo()).build();
        new PortsModel(taskInfo);
    }

    @Test
    public void shouldParsePortsCorrectly() {
        Protos.DiscoveryInfo.Builder discoveryInfo = getDefaultDiscoveryInfo()
                .setPorts(Protos.Ports.newBuilder()
                        .addPorts(Protos.Port.newBuilder()
                                .setName(Discovery.CLIENT_PORT_NAME)
                                .setNumber(1234))
                        .addPorts(Protos.Port.newBuilder()
                                .setName(Discovery.TRANSPORT_PORT_NAME)
                                .setNumber(12345)));
        PortsModel portsModel = new PortsModel(getDefaultTaskInfo(discoveryInfo).build());
        assertEquals(1234, portsModel.getClientPort().getNumber());
        assertEquals(12345, portsModel.getTransportPort().getNumber());
    }

    private Protos.DiscoveryInfo.Builder getDefaultDiscoveryInfo() {
        return Protos.DiscoveryInfo.newBuilder()
                .setVisibility(Protos.DiscoveryInfo.Visibility.EXTERNAL);
    }

    private Protos.TaskInfo.Builder getDefaultTaskInfo(Protos.DiscoveryInfo.Builder discoveryInfo) {
        return Protos.TaskInfo.newBuilder()
                .setName("")
                .setTaskId(Protos.TaskID.newBuilder().setValue("0"))
                .setSlaveId(Protos.SlaveID.newBuilder().setValue("0"))
                .setDiscovery(discoveryInfo);
    }
}