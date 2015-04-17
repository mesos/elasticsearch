package org.elasticsearch.cloud.mesos;

import org.elasticsearch.discovery.mesos.MesosUnicastHostsProvider;

import java.util.List;

/**
 * A singleton to manage all the tasks from mesos.
 */
public interface MesosStateService {
    List<String> getNodeIpsAndPorts(MesosUnicastHostsProvider mesosUnicastHostsProvider);
}
