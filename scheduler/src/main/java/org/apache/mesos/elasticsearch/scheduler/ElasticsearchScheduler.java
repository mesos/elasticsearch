package org.apache.mesos.elasticsearch.scheduler;

import org.apache.log4j.Logger;

import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import java.util.*;
import java.util.function.Predicate;

/**
 * Scheduler for Elasticsearch.
 */
@SuppressWarnings({"PMD.TooManyMethods"})
public class ElasticsearchScheduler implements Scheduler {

    public static final Logger LOGGER = Logger.getLogger(ElasticsearchScheduler.class.toString());

    private final Configuration configuration;

    private final TaskInfoFactory taskInfoFactory;

    Clock clock = new Clock();

    Set<Task> tasks = new HashSet<>();

    public ElasticsearchScheduler(Configuration configuration, TaskInfoFactory taskInfoFactory) {
        this.configuration = configuration;
        this.taskInfoFactory = taskInfoFactory;
    }

    public Set<Task> getTasks() {
        return tasks;
    }

    public void run() {
        LOGGER.info("Starting ElasticSearch on Mesos - [numHwNodes: " + configuration.getNumberOfHwNodes() + ", zk: " + configuration.getZookeeperPort() + " ]");

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

        final MesosSchedulerDriver driver = new MesosSchedulerDriver(this, frameworkBuilder.build(), "zk://" + configuration.getZookeeperHost() + ":" + configuration.getZookeeperPort() + "/mesos");
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
                Protos.TaskInfo taskInfo = taskInfoFactory.createTask(offer, configuration.getFrameworkId(), configuration);
                driver.launchTasks(Collections.singleton(offer.getId()), Collections.singleton(taskInfo));
                tasks.add(new Task(offer.getHostname(), taskInfo.getTaskId().getValue(), clock.zonedNow()));
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
        return tasks.stream().map(Task::getHostname).anyMatch(Predicate.isEqual(offer.getHostname()));
    }

}
