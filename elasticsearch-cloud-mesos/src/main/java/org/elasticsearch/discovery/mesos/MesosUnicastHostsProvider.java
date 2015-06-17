package org.elasticsearch.discovery.mesos;

import org.elasticsearch.Version;
import org.elasticsearch.cloud.mesos.MesosStateService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lang3.tuple.Pair;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.discovery.zen.ping.unicast.UnicastHostsProvider;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Provides a list of discovery nodes from the state.json data at the Mesos master.
 */
public class MesosUnicastHostsProvider extends AbstractComponent implements UnicastHostsProvider {

    private final MesosStateService mesosStateService;

    private final Version version;

    @Inject
    public MesosUnicastHostsProvider(Settings settings, MesosStateService mesosStateService, Version version) {
        super(settings);
        this.mesosStateService = mesosStateService;
        this.version = version;
    }

    @Override
    public List<DiscoveryNode> buildDynamicNodes() {
        List<Pair<String, Integer>> nodeIpsAndPorts = mesosStateService.getNodeIpsAndPorts(this);

        ArrayList<DiscoveryNode> discoveryNodes = Lists.newArrayList();
        for (Pair<String, Integer> nodeIpAndPort : nodeIpsAndPorts) {
            try {
                logger.debug("Creating discovery node from IP " + nodeIpAndPort);
                InetSocketAddress inetSocketAddress = new InetSocketAddress(nodeIpAndPort.getKey(), nodeIpAndPort.getValue());
                TransportAddress transportAddress = new InetSocketTransportAddress(inetSocketAddress);
                discoveryNodes.add(new DiscoveryNode("node-" + nodeIpAndPort, transportAddress, version.minimumCompatibilityVersion()));
            } catch (Exception e) {
                logger.debug("Could not create discoverynode", e);
            }
        }
        logger.debug("buildDynamicNodes - " + discoveryNodes);

        return discoveryNodes;
    }

}
