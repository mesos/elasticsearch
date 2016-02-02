package org.apache.mesos.elasticsearch.scheduler;

import org.apache.log4j.Logger;
import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.scheduler.state.ClusterState;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;

import static java.util.Arrays.asList;

/**
 * Offer strategy
 */
public class OfferStrategyNormal extends OfferStrategy {

    public OfferStrategyNormal(Configuration configuration, ClusterState clusterState) {
        super(configuration, clusterState);
        
        acceptanceRules = asList(
                new OfferRule("Host already running task", this::isHostAlreadyRunningTask),
                new OfferRule("Hostname is unresolveable", offer -> !isHostnameResolveable(offer.getHostname())),
                new OfferRule("First ES node is not responding", offer -> !isAtLeastOneESNodeRunning()),
                new OfferRule("Cluster size already fulfilled", offer -> clusterState.getTaskList().size() >= configuration.getElasticsearchNodes()),
                new OfferRule("Offer did not have 2 ports", offer -> !containsTwoPorts(offer.getResourcesList())),
                new OfferRule("The offer does not contain the user specified ports", offer -> !containsUserSpecifiedPorts(offer.getResourcesList())),
                new OfferRule("Offer did not have enough CPU resources", offer -> !isEnoughCPU(configuration, offer.getResourcesList())),
                new OfferRule("Offer did not have enough RAM resources", offer -> !isEnoughRAM(configuration, offer.getResourcesList())),
                new OfferRule("Offer did not have enough disk resources", offer -> !isEnoughDisk(configuration, offer.getResourcesList()))
        );
    }

    private boolean isEnoughDisk(Configuration configuration, List<Protos.Resource> resourcesList) {
        return new ResourceCheck(Resources.RESOURCE_DISK).isEnough(resourcesList, configuration.getDisk());
    }

}
