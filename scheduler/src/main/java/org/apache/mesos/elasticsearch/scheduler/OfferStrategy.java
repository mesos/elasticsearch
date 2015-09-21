package org.apache.mesos.elasticsearch.scheduler;

import org.apache.log4j.Logger;
import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.scheduler.state.ClusterState;

import java.util.List;
import java.util.Optional;

public class OfferStrategy {
    private static final Logger LOGGER = Logger.getLogger(ElasticsearchScheduler.class.toString());
    private ClusterState clusterState;
    private Configuration configuration;

    public OfferStrategy(Configuration configuration, ClusterState clusterState) {
        this.clusterState = clusterState;
        this.configuration = configuration;
    }

    public OfferResult evaluate(Protos.Offer offer) {
        if (isHostAlreadyRunningTask(clusterState, offer)) {
            LOGGER.info("Declined offer: Host " + offer.getHostname() + " is already running an Elastisearch task");
            return OfferResult.decline("Host already running task");
        } else if (clusterState.getTaskList().size() == configuration.getElasticsearchNodes()) {
            LOGGER.info("Declined offer: Mesos runs already runs " + configuration.getElasticsearchNodes() + " Elasticsearch tasks");
            return OfferResult.decline("Cluster size already fulfilled");
        } else if (!containsTwoPorts(offer.getResourcesList())) {
            LOGGER.info("Declined offer: Offer did not contain 2 ports for Elasticsearch client and transport connection");
            return OfferResult.decline("Offer did not have 2 ports");
        } else if (!isEnoughCPU(configuration, offer.getResourcesList())) {
            LOGGER.info("Declined offer: Not enough CPU resources");
            return OfferResult.decline("Offer did not have enough CPU resources");
        } else if (!isEnoughRAM(configuration, offer.getResourcesList())) {
            LOGGER.info("Declined offer: Not enough RAM resources");
            return OfferResult.decline("Offer did not have enough RAM resources");
        } else if (!isEnoughDisk(configuration, offer.getResourcesList())) {
            LOGGER.info("Not enough Disk resources");
            return OfferResult.decline("Offer did not have enough disk resources");
        }
        LOGGER.info("Accepted offer: " + offer.getHostname());
        return OfferResult.accept();
    }

    public static class OfferResult {
        final boolean accepted;
        final Optional<String> reason;

        private OfferResult(boolean accepted, Optional<String> reason) {
            this.accepted = accepted;
            this.reason = reason;
        }

        public static OfferResult accept() {
            return new OfferResult(true, Optional.<String>empty());
        }

        public static OfferResult decline(String reason) {
            return new OfferResult(false, Optional.of(reason));
        }
    }

    private boolean isHostAlreadyRunningTask(ClusterState clusterState, Protos.Offer offer) {
        Boolean result = false;
        List<Protos.TaskInfo> stateList = clusterState.getTaskList();
        for (Protos.TaskInfo t : stateList) {
            if (t.getSlaveId().equals(offer.getSlaveId())) {
                result = true;
            }
        }
        return result;
    }
    private boolean isEnoughDisk(Configuration configuration, List<Protos.Resource> resourcesList) {
        return new ResourceCheck(Resources.RESOURCE_DISK).isEnough(resourcesList, configuration.getDisk());
    }

    private boolean isEnoughCPU(Configuration configuration, List<Protos.Resource> resourcesList) {
        return new ResourceCheck(Resources.RESOURCE_CPUS).isEnough(resourcesList, configuration.getCpus());
    }

    private boolean isEnoughRAM(Configuration configuration, List<Protos.Resource> resourcesList) {
        return new ResourceCheck(Resources.RESOURCE_MEM).isEnough(resourcesList, configuration.getMem());
    }

    private boolean containsTwoPorts(List<Protos.Resource> resources) {
        return Resources.selectTwoPortsFromRange(resources).size() == 2;
    }

}
