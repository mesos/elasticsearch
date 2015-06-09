package org.apache.mesos.elasticsearch.common;

/**
 * Discovery mappings.
 */
public interface Discovery {
    // An elasticsearch task has 2 ports, client port and transport port.
    int CLIENT_PORT_INDEX = 0;
    int TRANSPORT_PORT_INDEX = 1;
    String CLIENT_PORT_NAME = "CLIENT_PORT";
    String TRANSPORT_PORT_NAME = "TRANSPORT_PORT";

}
