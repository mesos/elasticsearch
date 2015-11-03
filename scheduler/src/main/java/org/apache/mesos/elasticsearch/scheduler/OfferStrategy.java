package org.apache.mesos.elasticsearch.scheduler;

import org.apache.log4j.Logger;
import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.scheduler.state.ClusterState;

import java.util.List;
import java.util.Optional;

import static java.util.Arrays.asList;

/**
 * Offer strategy
 */
public class OfferStrategy {
    private static final Logger LOGGER = Logger.getLogger(ElasticsearchScheduler.class.toString());
    private ClusterState clusterState;
    private Configuration configuration;

    private List<OfferRule> acceptanceRules = asList(
            new OfferRule("Host already running task", this::isHostAlreadyRunningTask),
            new OfferRule("Cluster size already fulfilled", offer -> clusterState.getTaskList().size() == configuration.getElasticsearchNodes()),
            new OfferRule("Offer did not have 2 ports", offer -> !containsTwoPorts(offer.getResourcesList())),
            new OfferRule("Offer did not have enough CPU resources", offer -> !isEnoughCPU(configuration, offer.getResourcesList())),
            new OfferRule("Offer did not have enough RAM resources", offer -> !isEnoughRAM(configuration, offer.getResourcesList())),
            new OfferRule("Offer did not have enough disk resources", offer -> !isEnoughDisk(configuration, offer.getResourcesList()))
    );

    public OfferStrategy(Configuration configuration, ClusterState clusterState) {
        this.clusterState = clusterState;
        this.configuration = configuration;
    }

    public OfferResult evaluate(Protos.Offer offer) {
        final Optional<OfferRule> decline = acceptanceRules.stream().filter(offerRule -> offerRule.rule.accepts(offer)).limit(1).findFirst();
        if (decline.isPresent()) {
            return OfferResult.decline(decline.get().declineReason);
        }

        LOGGER.info("Accepted offer: " + offer.getHostname());
        return OfferResult.accept();
    }

    /**
     * Offer result
     */
    public static class OfferResult {
        final boolean acceptable;
        final Optional<String> reason;

        private OfferResult(boolean acceptable, Optional<String> reason) {
            this.acceptable = acceptable;
            this.reason = reason;
        }

        public static OfferResult accept() {
            return new OfferResult(true, Optional.<String>empty());
        }

        public static OfferResult decline(String reason) {
            return new OfferResult(false, Optional.of(reason));
        }
    }

    private boolean isHostAlreadyRunningTask(Protos.Offer offer) {
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

    /**
     * Rule and reason container object
     */
    private static class OfferRule {
        String declineReason;
        Rule rule;

        public OfferRule(String declineReason, Rule rule) {
            this.declineReason = declineReason;
            this.rule = rule;
        }
    }

    /**
     * Interface for checking offers
     */
    @FunctionalInterface
    private interface Rule {
        boolean accepts(Protos.Offer offer);
    }
}
