package org.apache.mesos.elasticsearch.scheduler;

import org.apache.log4j.Logger;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.elasticsearch.common.Discovery;
import org.apache.mesos.elasticsearch.scheduler.cluster.ClusterMonitor;
import org.apache.mesos.elasticsearch.scheduler.state.ClusterState;
import org.apache.mesos.elasticsearch.scheduler.state.FrameworkState;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.ZonedDateTime;
import java.util.*;

import static java.util.stream.Collectors.toList;

/**
 * Scheduler for Elasticsearch.
 */
@SuppressWarnings({"PMD.TooManyMethods"})
public class ElasticsearchScheduler implements Scheduler {

    private static final Logger LOGGER = Logger.getLogger(ElasticsearchScheduler.class.toString());

    private final Configuration configuration;

    private final TaskInfoFactory taskInfoFactory;

    private ClusterMonitor clusterMonitor = null;

    private Observable statusUpdateWatchers = new StatusUpdateObservable();
    private Boolean registered = false;

    public ElasticsearchScheduler(Configuration configuration, TaskInfoFactory taskInfoFactory) {
        this.configuration = configuration;
        this.taskInfoFactory = taskInfoFactory;
    }

    public List<Task> getTasks() {
        FrameworkState frameworkState = new FrameworkState(configuration.getState());
        frameworkState.setFrameworkId(configuration.getFrameworkId());
        configuration.setFrameworkState(frameworkState);

        ClusterState clusterState = new ClusterState(configuration.getState(), frameworkState);
        return clusterState.getTaskList().stream().map(taskInfo -> {
            Properties data = new Properties();
            try {
                data.load(taskInfo.getData().newInput());
            } catch (IOException e) {
                throw new RuntimeException("Failed to parse properties", e);
            }
            String hostName = data.getProperty("hostname", "UNKNOWN");
            String ipAddress = data.getProperty("ipAddress", hostName);
            ZonedDateTime startedAt = ZonedDateTime.parse(data.getProperty("startedAt", ZonedDateTime.now().toString()));

            return new Task(
                    hostName,
                    taskInfo.getTaskId().getValue(),
                    Protos.TaskState.TASK_STARTING, //TODO: fetch correct state
                    startedAt,
                    new InetSocketAddress(ipAddress, taskInfo.getDiscovery().getPorts().getPorts(Discovery.CLIENT_PORT_INDEX).getNumber()),
                    new InetSocketAddress(ipAddress, taskInfo.getDiscovery().getPorts().getPorts(Discovery.TRANSPORT_PORT_INDEX).getNumber()));
        }).collect(toList());
    }

    public void run() {
        LOGGER.info("Starting ElasticSearch on Mesos - [numHwNodes: " + configuration.getElasticsearchNodes() +
                ", zk mesos: " + configuration.getMesosZKURL() +
                ", zk framework: " + configuration.getFrameworkZKURL() +
                ", ram:" + configuration.getMem() + "]");

        FrameworkInfoFactory frameworkInfoFactory = new FrameworkInfoFactory(configuration);
        final Protos.FrameworkInfo.Builder frameworkBuilder = frameworkInfoFactory.getBuilder();

        final MesosSchedulerDriver driver = new MesosSchedulerDriver(this, frameworkBuilder.build(), configuration.getMesosZKURL());

        driver.run();
    }

    @Override
    public void registered(SchedulerDriver driver, Protos.FrameworkID frameworkId, Protos.MasterInfo masterInfo) {
        FrameworkState frameworkState = new FrameworkState(configuration.getState());
        frameworkState.setFrameworkId(frameworkId);
        configuration.setFrameworkState(frameworkState); // DCOS certification 02

        LOGGER.info("Framework registered as " + frameworkId.getValue());

        ClusterState clusterState = new ClusterState(configuration.getState(), frameworkState); // Must use new framework state. This is when we are allocated our FrameworkID.

        clusterMonitor = new ClusterMonitor(configuration, this, driver, clusterState);
        statusUpdateWatchers.addObserver(clusterMonitor);

        List<Protos.Resource> resources = Resources.buildFrameworkResources(configuration);

        Protos.Request request = Protos.Request.newBuilder()
                .addAllResources(resources)
                .build();

        List<Protos.Request> requests = Collections.singletonList(request);
        driver.requestResources(requests);
        registered = true;
    }

    @Override
    public void reregistered(SchedulerDriver driver, Protos.MasterInfo masterInfo) {
        LOGGER.info("Framework re-registered");
    }

    // Todo, this massive if statement needs to be performed better.
    @Override
    public void resourceOffers(SchedulerDriver driver, List<Protos.Offer> offers) {
        if (!registered) {
            LOGGER.debug("Not registered, can't accept resource offers.");
            return;
        }
        for (Protos.Offer offer : offers) {
            if (isHostAlreadyRunningTask(offer)) {
                driver.declineOffer(offer.getId()); // DCOS certification 05
                LOGGER.info("Declined offer: Host " + offer.getHostname() + " is already running an Elastisearch task");
            } else if (clusterMonitor.getClusterState().getTaskList().size() == configuration.getElasticsearchNodes()) {
                driver.declineOffer(offer.getId()); // DCOS certification 05
                LOGGER.info("Declined offer: Mesos runs already runs " + configuration.getElasticsearchNodes() + " Elasticsearch tasks");
            } else if (!containsTwoPorts(offer.getResourcesList())) {
                LOGGER.info("Declined offer: Offer did not contain 2 ports for Elasticsearch client and transport connection");
                driver.declineOffer(offer.getId());
            } else if (!isEnoughCPU(offer.getResourcesList())) {
                LOGGER.info("Declined offer: Not enough CPU resources");
                driver.declineOffer(offer.getId());
            } else if (!isEnoughRAM(offer.getResourcesList())) {
                LOGGER.info("Declined offer: Not enough RAM resources");
                driver.declineOffer(offer.getId());
            } else if (!isEnoughDisk(offer.getResourcesList())) {
                LOGGER.info("Not enough Disk resources");
                driver.declineOffer(offer.getId());
            } else {
                LOGGER.info("Accepted offer: " + offer.getHostname());
                Protos.TaskInfo taskInfo = taskInfoFactory.createTask(configuration, offer);
                LOGGER.debug(taskInfo.toString());
                driver.launchTasks(Collections.singleton(offer.getId()), Collections.singleton(taskInfo));
                clusterMonitor.monitorTask(taskInfo); // Add task to cluster monitor
            }
        }
    }

    private boolean isEnoughDisk(List<Protos.Resource> resourcesList) {
        ResourceCheck resourceCheck = new ResourceCheck(Resources.RESOURCE_DISK);
        return resourceCheck.isEnough(resourcesList, configuration.getDisk());
    }

    private boolean isEnoughCPU(List<Protos.Resource> resourcesList) {
        ResourceCheck resourceCheck = new ResourceCheck(Resources.RESOURCE_CPUS);
        return resourceCheck.isEnough(resourcesList, configuration.getCpus());
    }

    private boolean isEnoughRAM(List<Protos.Resource> resourcesList) {
        ResourceCheck resourceCheck = new ResourceCheck(Resources.RESOURCE_MEM);
        return resourceCheck.isEnough(resourcesList, configuration.getMem());
    }

    private boolean containsTwoPorts(List<Protos.Resource> resources) {
        int count = Resources.selectTwoPortsFromRange(resources).size();
        return count == 2;
    }

    @Override
    public void offerRescinded(SchedulerDriver driver, Protos.OfferID offerId) {
        LOGGER.info("Offer " + offerId.getValue() + " rescinded");
    }

    @Override
    public void statusUpdate(SchedulerDriver driver, Protos.TaskStatus status) {
        LOGGER.info("Status update - Task with ID '" + status.getTaskId().getValue() + "' is now in state '" + status.getState() + "'. Message: " + status.getMessage());
        statusUpdateWatchers.notifyObservers(status);
    }

    @Override
    public void frameworkMessage(SchedulerDriver driver, Protos.ExecutorID executorId, Protos.SlaveID slaveId, byte[] data) {
        LOGGER.info("Framework Message - Executor: " + executorId.getValue() + ", SlaveID: " + slaveId.getValue());
    }

    @Override
    public void disconnected(SchedulerDriver driver) {
        LOGGER.warn("Disconnected");
    }

    @Override
    public void slaveLost(SchedulerDriver driver, Protos.SlaveID slaveId) {
        LOGGER.info("Slave lost: " + slaveId.getValue());
    }

    @Override
    public void executorLost(SchedulerDriver driver, Protos.ExecutorID executorId, Protos.SlaveID slaveId, int status) {
        // This is never called by Mesos, so we have to call it ourselves via a healthcheck
        // https://issues.apache.org/jira/browse/MESOS-313
        LOGGER.info("Executor lost: " + executorId.getValue() +
                "on slave " + slaveId.getValue() +
                "with status " + status);
    }

    @Override
    public void error(SchedulerDriver driver, String message) {
        LOGGER.error("Error: " + message);
    }

    private boolean isHostAlreadyRunningTask(Protos.Offer offer) {
        Boolean result = false;
        List<Protos.TaskInfo> stateList = clusterMonitor.getClusterState().getTaskList();
        for (Protos.TaskInfo t : stateList) {
            if (t.getSlaveId().equals(offer.getSlaveId())) {
                result = true;
            }
        }
        return result;
    }

    /**
     * Implementation of Observable to fix the setChanged problem.
     */
    private static class StatusUpdateObservable extends Observable {
        @Override
        public void notifyObservers(Object arg) {
            this.setChanged(); // This is ridiculous.
            super.notifyObservers(arg);
        }
    }
}
