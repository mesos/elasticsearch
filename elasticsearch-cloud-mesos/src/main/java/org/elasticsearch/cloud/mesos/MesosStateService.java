package org.elasticsearch.cloud.mesos;

import org.elasticsearch.common.lang3.tuple.Pair;
import org.elasticsearch.discovery.mesos.MesosUnicastHostsProvider;

import java.util.List;

/**
 * A singleton to manage all the tasks from mesos.
 */
public interface MesosStateService {
    List<Pair<String, Integer>> getNodeIpsAndPorts(MesosUnicastHostsProvider mesosUnicastHostsProvider);
}
