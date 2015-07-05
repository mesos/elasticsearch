package org.apache.mesos.elasticsearch.common;

/**
 * Discovery mappings.
 */
public class Discovery {
    // An elasticsearch task has 2 ports, client port and transport port.
    public static final int CLIENT_PORT_INDEX = 0;
    public static final int TRANSPORT_PORT_INDEX = 1;
    public static final String CLIENT_PORT_NAME = "CLIENT_PORT";
    public static final String TRANSPORT_PORT_NAME = "TRANSPORT_PORT";
    public static final int EXPECTED_NUMBER_OF_PORTS = 2;
}
