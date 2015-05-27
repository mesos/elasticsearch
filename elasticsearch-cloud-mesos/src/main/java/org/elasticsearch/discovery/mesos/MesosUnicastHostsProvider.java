package org.elasticsearch.discovery.mesos;

import org.elasticsearch.Version;
import org.elasticsearch.cloud.mesos.MesosStateService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.discovery.zen.ping.unicast.UnicastHostsProvider;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Arrays;
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
        for (String nodeIp : nodeIps) {
            try {
                logger.debug("Creating discovery node from IP " + nodeIp);
                TransportAddress[] transportAddresses = transportService.addressesFromString(nodeIp);
                discoveryNodes.add(new DiscoveryNode("node-" + nodeIp, transportAddresses[0], version.minimumCompatibilityVersion()));
            } catch (Exception e) {
                logger.debug("Could not create discoverynode", e);
            }
        }
        logger.debug("buildDynamicNodes - " + discoveryNodes);

        return discoveryNodes;
    }

}
