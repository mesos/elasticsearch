package org.elasticsearch.discovery.mesos;

import org.elasticsearch.Version;
import org.elasticsearch.cloud.mesos.MesosStateService;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNodeService;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.discovery.zen.ZenDiscovery;
import org.elasticsearch.discovery.zen.elect.ElectMasterService;
import org.elasticsearch.discovery.zen.ping.ZenPing;
import org.elasticsearch.discovery.zen.ping.ZenPingService;
import org.elasticsearch.discovery.zen.ping.unicast.UnicastZenPing;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Optional;

/**
 *  ES discovery implementation for Mesos.
 */
@SuppressWarnings("PMD.ExcessiveParameterList")
public class MesosDiscovery extends ZenDiscovery {

    @Inject
    public MesosDiscovery(Settings settings,
        ClusterName clusterName, ThreadPool threadPool, TransportService transportService, ClusterService clusterService,
        NodeSettingsService nodeSettingsService, DiscoveryNodeService discoveryNodeService, MesosStateService mesosStateService, ZenPingService pingService,
        ElectMasterService electMasterService, DiscoverySettings discoverySettings) {

        super(settings, clusterName, threadPool, transportService, clusterService, nodeSettingsService,
            discoveryNodeService, pingService, electMasterService, discoverySettings);

        if (settings.getAsBoolean("cloud.enabled", true)) {
            ImmutableList<? extends ZenPing> zenPings = pingService.zenPings();
            final Optional<UnicastZenPing> unicastZenPing = zenPings.stream()
                    .filter(zenPing -> zenPing instanceof UnicastZenPing)
                    .map(zenPing -> (UnicastZenPing) zenPing)
                    .findFirst();

            if (unicastZenPing.isPresent()) {
                // update the unicast zen ping to add cloud hosts provider
                // and, while we are at it, use only it and not the multicast for example
                unicastZenPing.get().addHostsProvider(new MesosUnicastHostsProvider(settings, mesosStateService, Version.V_1_4_0));
                pingService.zenPings(ImmutableList.of(unicastZenPing.get()));
            } else {
                logger.warn("Failed to apply gce unicast discovery, no unicast ping found");
            }
        }
    }
}
