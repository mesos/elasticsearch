package org.elasticsearch.discovery.mesos;

import org.elasticsearch.Version;
import org.elasticsearch.cloud.mesos.MesosStateService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.zen.ping.unicast.UnicastHostsProvider;
import org.elasticsearch.transport.TransportService;

import java.util.List;

import static java.util.stream.Collectors.toList;

/**
 * Provides a list of discovery nodes from the state.json data at the Mesos master.
 */
public class MesosUnicastHostsProvider extends AbstractComponent implements UnicastHostsProvider {
    private final MesosStateService mesosStateService;

    private final TransportService transportService;

    private final Version version;


    @Inject
    public MesosUnicastHostsProvider(Settings settings, MesosStateService mesosStateService, TransportService transportService, Version version) {
        super(settings);
        this.mesosStateService = mesosStateService;
        this.transportService = transportService;
        this.version = version;
    }

    @Override
    public List<DiscoveryNode> buildDynamicNodes() {
        final List<DiscoveryNode> discoveryNodes = mesosStateService.getNodeIpsAndPorts(this).stream().map(this::toDiscoveryNode).collect(toList());

        logger.debug("buildDynamicNodes - ", discoveryNodes);

        return discoveryNodes;
    }

    private DiscoveryNode toDiscoveryNode(String nodeIp) {
        try {
            return new DiscoveryNode("node-" + nodeIp, transportService.addressesFromString(nodeIp)[0], version.minimumCompatibilityVersion());
        } catch (Exception e) {
            throw new RuntimeException("Could not create discoverynode", e);
        }
    }

}
