package org.apache.mesos.elasticsearch.scheduler;

import org.apache.log4j.Logger;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.elasticsearch.scheduler.cluster.ClusterMonitor;
import org.apache.mesos.elasticsearch.scheduler.state.ClusterState;
import org.apache.mesos.elasticsearch.scheduler.state.ESTaskStatus;
import org.apache.mesos.elasticsearch.scheduler.state.FrameworkState;
import org.apache.mesos.elasticsearch.scheduler.state.StatePath;

import java.util.*;

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
    private ClusterState clusterState;
    OfferStrategy offerStrategy;

    private SchedulerDriver schedulerDriver;

    public ElasticsearchScheduler(Configuration configuration, TaskInfoFactory taskInfoFactory) {
        this.configuration = configuration;
        this.taskInfoFactory = taskInfoFactory;
    }

    public Map<String, Task> getTasks() {
        if (clusterState == null) {
            return new HashMap<>();
        } else {
            return clusterState.getGuiTaskList();
        }
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
        this.schedulerDriver = driver;

        FrameworkState frameworkState = new FrameworkState(configuration.getState());
        frameworkState.setFrameworkId(frameworkId);
        configuration.setFrameworkState(frameworkState); // DCOS certification 02

        LOGGER.info("Framework registered as " + frameworkId.getValue());

        clusterState = new ClusterState(configuration.getState(), frameworkState); // Must use new framework state. This is when we are allocated our FrameworkID.
        offerStrategy = new OfferStrategy(configuration, clusterState);
        clusterMonitor = new ClusterMonitor(configuration, this, driver, new StatePath(configuration.getState()));
        clusterState.getTaskList().forEach(clusterMonitor::startMonitoringTask); // Get all previous executors and start monitoring them.
        statusUpdateWatchers.addObserver(clusterState);
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

    @Override
    public void resourceOffers(SchedulerDriver driver, List<Protos.Offer> offers) {
        if (!registered) {
            LOGGER.debug("Not registered, can't accept resource offers.");
            return;
        }

        // TODO (jhf@trifork.com): This should happen immediately after the target number of nodes has been changed.
        // This current behavior is not correct, e.g. it can cause a system "deadlock" if we are using all Mesos
        // resources but wish to scale down (Mesos will never give us any new offers, so we will never relinquish any
        // resources).
        removeExcessElasticsearchNodes();

        for (Protos.Offer offer : offers) {
            final OfferStrategy.OfferResult result = offerStrategy.evaluate(offer);

            if (!result.acceptable) {
                LOGGER.debug("Declined offer: " + result.reason.orElse("Unknown"));
                driver.declineOffer(offer.getId());
            } else {
                Protos.TaskInfo taskInfo = taskInfoFactory.createTask(configuration, offer);
                LOGGER.debug(taskInfo.toString());
                driver.launchTasks(Collections.singleton(offer.getId()), Collections.singleton(taskInfo));
                ESTaskStatus esTask = new ESTaskStatus(configuration.getState(), configuration.getFrameworkId(), taskInfo, new StatePath(configuration.getState())); // Write staging state to zk
                clusterState.addTask(esTask); // Add tasks to cluster state and write to zk
                clusterMonitor.startMonitoringTask(esTask); // Add task to cluster monitor
            }
        }
    }

    public void removeExcessElasticsearchNodes() {
        while (clusterState.getTaskList().size() > configuration.getElasticsearchNodes()) {
            killLastStartedExecutor();
        }
    }

    private void killLastStartedExecutor() {
        List<Protos.TaskInfo> taskList = clusterState.getTaskList();
        int size = taskList.size();
        Protos.TaskInfo killTaskInfo = taskList.get(size - 1);
        Protos.TaskID killTaskId = killTaskInfo.getTaskId();
        LOGGER.debug("Killing task: " + killTaskId);
        schedulerDriver.killTask(killTaskId);
        while (clusterState.getTaskList().stream().filter(taskInfo -> taskInfo.getTaskId().equals(killTaskId)).count() > 0) {
            LOGGER.debug("Waiting for task to be removed from list: " + killTaskId);
        }
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
                " on slave " + slaveId.getValue() +
                " with status " + status);
        try {
            Protos.TaskInfo taskInfo = clusterState.getTask(executorId);
            statusUpdate(driver, Protos.TaskStatus.newBuilder().setExecutorId(executorId).setSlaveId(slaveId).setTaskId(taskInfo.getTaskId()).setState(Protos.TaskState.TASK_LOST).build());
            driver.killTask(taskInfo.getTaskId()); // It may not actually be lost, it may just have hanged. So Kill, just in case.
        } catch (IllegalArgumentException e) {
            LOGGER.warn("Unable to find TaskInfo with the given Executor ID", e);
        }
    }

    @Override
    public void error(SchedulerDriver driver, String message) {
        LOGGER.error("Error: " + message);
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
