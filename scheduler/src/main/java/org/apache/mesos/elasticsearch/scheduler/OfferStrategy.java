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
public class OfferStrategy {
    protected static final Logger LOGGER = Logger.getLogger(ElasticsearchScheduler.class.toString());
    public static final int DUMMY_PORT = 80;
    protected ClusterState clusterState;
    protected Configuration configuration;

    protected List<OfferRule> acceptanceRules = null;

    protected boolean isHostnameResolveable(String hostname) {
        LOGGER.debug("Attempting to resolve hostname: " + hostname);
        InetSocketAddress address = new InetSocketAddress(hostname, DUMMY_PORT);
        return !address.isUnresolved();
    }

    protected boolean isAtLeastOneESNodeRunning() {
        // If this is the first, do not check
        List<Protos.TaskInfo> taskList = clusterState.getTaskList();
        if (taskList.size() == 0) {
            return true;
        } else {
            return clusterState.getStatus(taskList.get(0).getTaskId()).getStatus().getState().equals(Protos.TaskState.TASK_RUNNING);
        }
    }

    protected OfferStrategy(Configuration configuration, ClusterState clusterState) {
        this.clusterState = clusterState;
        this.configuration = configuration;
    }

    protected OfferResult evaluate(Protos.Offer offer) {
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
    protected static class OfferResult {
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

    protected boolean isHostAlreadyRunningTask(Protos.Offer offer) {
        Boolean result = false;
        List<Protos.TaskInfo> stateList = clusterState.getTaskList();
        for (Protos.TaskInfo t : stateList) {
            if (t.getSlaveId().equals(offer.getSlaveId())) {
                result = true;
            }
        }
        return result;
    }

    protected boolean isEnoughCPU(Configuration configuration, List<Protos.Resource> resourcesList) {
        return new ResourceCheck(Resources.RESOURCE_CPUS).isEnough(resourcesList, configuration.getCpus());
    }

    protected boolean isEnoughRAM(Configuration configuration, List<Protos.Resource> resourcesList) {
        return new ResourceCheck(Resources.RESOURCE_MEM).isEnough(resourcesList, configuration.getMem());
    }

    protected boolean containsTwoPorts(List<Protos.Resource> resources) {
        return Resources.selectTwoPortsFromRange(resources).size() == 2;
    }

    protected boolean containsUserSpecifiedPorts(List<Protos.Resource> resourcesList) {
        // If there are user specified ports, check each port is contained within the offer
        if (!configuration.getElasticsearchPorts().isEmpty()) {
            for (Integer port : configuration.getElasticsearchPorts()) {
                if (!Resources.isPortAvailable(resourcesList, port)) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Rule and reason container object
     */
    protected static class OfferRule {
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
    protected interface Rule {
        boolean accepts(Protos.Offer offer);
    }
}
