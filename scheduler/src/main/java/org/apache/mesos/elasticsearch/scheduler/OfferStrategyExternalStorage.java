package org.apache.mesos.elasticsearch.scheduler;

import org.apache.log4j.Logger;
import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.scheduler.state.ClusterState;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;

import static java.util.Arrays.asList;

/**
 * Offer strategy when external storage is involved. Notice when compared to the OfferStrategyNormal, the OfferRule for
 * checking if enough storage space is no longer needed because external volumes size is managed externally (storage array, Amazon EBS, etc).
 */
public class OfferStrategyExternalStorage extends OfferStrategy {

    public OfferStrategyExternalStorage(Configuration configuration, ClusterState clusterState) {
        super(configuration, clusterState);
        
        acceptanceRules = asList(
                new OfferRule("Host already running task", this::isHostAlreadyRunningTask),
                new OfferRule("Hostname is unresolveable", offer -> !isHostnameResolveable(offer.getHostname())),
                new OfferRule("First ES node is not responding", offer -> !isAtLeastOneESNodeRunning()),
                new OfferRule("Cluster size already fulfilled", offer -> clusterState.getTaskList().size() >= configuration.getElasticsearchNodes()),
                new OfferRule("Offer did not have 2 ports", offer -> !containsTwoPorts(offer.getResourcesList())),
                new OfferRule("The offer does not contain the user specified ports", offer -> !containsUserSpecifiedPorts(offer.getResourcesList())),
                new OfferRule("Offer did not have enough CPU resources", offer -> !isEnoughCPU(configuration, offer.getResourcesList())),
                new OfferRule("Offer did not have enough RAM resources", offer -> !isEnoughRAM(configuration, offer.getResourcesList()))
        );
    }

}
