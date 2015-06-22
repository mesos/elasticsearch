package org.apache.mesos.elasticsearch.executor.model;

import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.common.Discovery;
import org.apache.mesos.elasticsearch.executor.parser.ParsePorts;
import org.apache.mesos.elasticsearch.executor.parser.TaskParser;

import java.security.InvalidAlgorithmParameterException;
import java.util.List;

/**
 * Model of the port allocation.
 */
public class PortsModel {
    private TaskParser<List<Protos.Port>> parser = new ParsePorts();
    private final List<Protos.Port> portsList;
    private final Protos.Port clientPort;
    private final Protos.Port transportPort;

    public PortsModel(Protos.TaskInfo taskInfo) throws InvalidAlgorithmParameterException {
        portsList = parser.parse(taskInfo);
        clientPort = portsList.get(Discovery.CLIENT_PORT_INDEX);
        transportPort = portsList.get(Discovery.TRANSPORT_PORT_INDEX);
    }

    public Protos.Port getClientPort() {
        return clientPort;
    }

    public Protos.Port getTransportPort() {
        return transportPort;
    }
}
