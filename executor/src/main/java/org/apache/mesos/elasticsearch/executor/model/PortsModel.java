package org.apache.mesos.elasticsearch.executor.model;

import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.common.Discovery;
import org.apache.mesos.elasticsearch.executor.parser.ParsePorts;
import org.apache.mesos.elasticsearch.executor.parser.TaskParser;
import org.elasticsearch.common.settings.Settings;

import java.security.InvalidParameterException;
import java.util.List;

/**
 * Model of the port allocation.
 */
public class PortsModel implements RunTimeSettings {
    public static final String HTTP_PORT_KEY = "http.port";
    public static final String TRANSPORT_PORT_KEY = "transport.tcp.port";
    private TaskParser<List<Protos.Port>> parser = new ParsePorts();
    private final List<Protos.Port> portsList;
    private final Protos.Port clientPort;
    private final Protos.Port transportPort;

    public PortsModel(Protos.TaskInfo taskInfo) throws InvalidParameterException, NullPointerException {
        portsList = parser.parse(taskInfo);
        if (portsList.size() != Discovery.EXPECTED_NUMBER_OF_PORTS) {
            throw new InvalidParameterException("DiscoveryInfo packet must contain " + Integer.toString(Discovery.EXPECTED_NUMBER_OF_PORTS) + " ports.");
        }
        clientPort = portsList.get(Discovery.CLIENT_PORT_INDEX);
        transportPort = portsList.get(Discovery.TRANSPORT_PORT_INDEX);
    }

    private Settings.Builder getClientPort() {
        return Settings.settingsBuilder().put(HTTP_PORT_KEY, String.valueOf(clientPort.getNumber()));

    }

    private Settings.Builder getTransportPort() {
        return Settings.settingsBuilder().put(TRANSPORT_PORT_KEY, String.valueOf(transportPort.getNumber()));
    }

    @Override
    public Settings.Builder getRuntimeSettings() {
        return Settings.settingsBuilder()
                .put(getClientPort().build())
                .put(getTransportPort().build());
    }
}
