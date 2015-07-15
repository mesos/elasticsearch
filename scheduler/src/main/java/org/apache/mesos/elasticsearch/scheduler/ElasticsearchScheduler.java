package org.apache.mesos.elasticsearch.scheduler;

import org.apache.log4j.Logger;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.elasticsearch.common.Discovery;
import java.net.InetSocketAddress;
import java.util.*;

/**
 * Scheduler for Elasticsearch.
 */
@SuppressWarnings({"PMD.TooManyMethods"})
public class ElasticsearchScheduler implements Scheduler {

    private static final Logger LOGGER = Logger.getLogger(ElasticsearchScheduler.class.toString());

    private final Configuration configuration;

    private final TaskInfoFactory taskInfoFactory;

    Clock clock = new Clock();

    Map<String, Task> tasks = new HashMap<String, Task>();

    public ElasticsearchScheduler(Configuration configuration, TaskInfoFactory taskInfoFactory) {
        this.configuration = configuration;
        this.taskInfoFactory = taskInfoFactory;
    }

    public Map<String, Task> getTasks() {
        return tasks;
    }

    public void run() {
        LOGGER.info("Starting ElasticSearch on Mesos - [numHwNodes: " + configuration.getNumberOfHwNodes() + ", zk: " + configuration.getZookeeperUrl() + "]");

        final Protos.FrameworkInfo.Builder frameworkBuilder = Protos.FrameworkInfo.newBuilder();
        frameworkBuilder.setUser("");
        frameworkBuilder.setName(configuration.getFrameworkName());
        frameworkBuilder.setCheckpoint(true);
        frameworkBuilder.setFailoverTimeout(configuration.getFailoverTimeout());
        frameworkBuilder.setCheckpoint(true); // DCOS certification 04 - Checkpointing is enabled.

        Protos.FrameworkID frameworkID = configuration.getFrameworkId(); // DCOS certification 02
        if (frameworkID != null) {
            LOGGER.info("Found previous frameworkID: " + frameworkID);
            frameworkBuilder.setId(frameworkID);
        }

        final MesosSchedulerDriver driver = new MesosSchedulerDriver(this, frameworkBuilder.build(), configuration.getZookeeperUrl());
        driver.run();
    }

    @Override
    public void registered(SchedulerDriver driver, Protos.FrameworkID frameworkId, Protos.MasterInfo masterInfo) {
        configuration.setFrameworkId(frameworkId); // DCOS certification 02

        LOGGER.info("Framework registered as " + frameworkId.getValue());

        List<Protos.Resource> resources = Resources.buildFrameworkResources();

        Protos.Request request = Protos.Request.newBuilder()
                .addAllResources(resources)
                .build();

        List<Protos.Request> requests = Collections.singletonList(request);
        driver.requestResources(requests);
    }

    @Override
    public void reregistered(SchedulerDriver driver, Protos.MasterInfo masterInfo) {
        LOGGER.info("Framework re-registered");
    }

    @Override
    public void resourceOffers(SchedulerDriver driver, List<Protos.Offer> offers) {
        for (Protos.Offer offer : offers) {
            if (isHostAlreadyRunningTask(offer)) {
                driver.declineOffer(offer.getId()); // DCOS certification 05
                LOGGER.info("Declined offer: Host " + offer.getHostname() + " is already running an Elastisearch task");
            } else if (tasks.size() == configuration.getNumberOfHwNodes()) {
                driver.declineOffer(offer.getId()); // DCOS certification 05
                LOGGER.info("Declined offer: Mesos runs already runs " + configuration.getNumberOfHwNodes() + " Elasticsearch tasks");
            } else if (!containsTwoPorts(offer.getResourcesList())) {
                LOGGER.info("Declined offer: Offer did not contain 2 ports for Elasticsearch client and transport connection");
                driver.declineOffer(offer.getId());
            } else {
                LOGGER.info("Accepted offer: " + offer.getHostname());
                Protos.TaskInfo taskInfo = taskInfoFactory.createTask(configuration, offer);
                driver.launchTasks(Collections.singleton(offer.getId()), Collections.singleton(taskInfo));
                Task task = new Task(
                    offer.getHostname(),
                    taskInfo.getTaskId().getValue(),
                    Protos.TaskState.TASK_STAGING,
                    clock.zonedNow(),
                    new InetSocketAddress(offer.getHostname(), taskInfo.getDiscovery().getPorts().getPorts(Discovery.CLIENT_PORT_INDEX).getNumber()),
                    new InetSocketAddress(offer.getHostname(), taskInfo.getDiscovery().getPorts().getPorts(Discovery.TRANSPORT_PORT_INDEX).getNumber())
                );
                tasks.put(taskInfo.getTaskId().getValue(), task);
            }
        }
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
        LOGGER.info("Status update - Task ID: " + status.getTaskId() + ", State: " + status.getState());
        Task task = tasks.get(status.getTaskId().getValue());
        if (task != null) {
            task.setState(status.getState());
        } else {
            throw new RuntimeException("Cannot update status of Task ID: " + status.getTaskId() + ". Unknown Task.");
        }
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
        LOGGER.info("Executor lost: " + executorId.getValue() +
                "on slave " + slaveId.getValue() +
                "with status " + status);
    }

    @Override
    public void error(SchedulerDriver driver, String message) {
        LOGGER.error("Error: " + message);
    }

    private boolean isHostAlreadyRunningTask(Protos.Offer offer) {
        return tasks.containsKey(offer.getId().getValue());
    }

}
