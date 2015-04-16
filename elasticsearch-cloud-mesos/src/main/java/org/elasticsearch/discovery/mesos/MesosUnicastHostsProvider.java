package org.elasticsearch.discovery.mesos;

import org.elasticsearch.Version;
import org.elasticsearch.cloud.mesos.MesosStateService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.zen.ping.unicast.UnicastHostsProvider;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;

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
        List<String> nodeIps = mesosStateService.getNodeIpsAndPorts(this);

        ArrayList<DiscoveryNode> discoveryNodes = Lists.newArrayList();
        logger.debug("slave ip={}", nodeIps);
        for (String nodeIp : nodeIps) {
            try {
                discoveryNodes.add(new DiscoveryNode("node-" + nodeIp, transportService.addressesFromString(nodeIp)[0], version.minimumCompatibilityVersion()));
            } catch (Exception e) {
                throw new RuntimeException("Could not create discoverynode", e);
            }
        }

        return discoveryNodes;
    }

}
