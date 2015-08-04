package org.apache.mesos.elasticsearch.scheduler.cluster;

import org.apache.commons.collections4.map.MultiValueMap;
import org.apache.log4j.Logger;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.elasticsearch.scheduler.Configuration;
import org.apache.mesos.elasticsearch.scheduler.healthcheck.BumpExecutor;
import org.apache.mesos.elasticsearch.scheduler.healthcheck.ExecutorHealth;
import org.apache.mesos.elasticsearch.scheduler.healthcheck.ExecutorHealthCheck;
import org.apache.mesos.elasticsearch.scheduler.healthcheck.PollService;
import org.apache.mesos.elasticsearch.scheduler.state.ClusterState;
import org.apache.mesos.elasticsearch.scheduler.state.ESTaskStatus;

import java.util.Observable;
import java.util.Observer;

/**
 * Contains all cluster information. Monitors state of cluster elements.
 */
public class ClusterMonitor implements Observer {
    private static final Logger LOGGER = Logger.getLogger(ClusterMonitor.class);
    private final Configuration configuration;
    private final Scheduler callback;
    private final SchedulerDriver driver;
    private final ClusterState clusterState;
    private MultiValueMap<Protos.TaskInfo, ExecutorHealthCheck> pollList = new MultiValueMap<>();

    public ClusterMonitor(Configuration configuration, Scheduler callback, SchedulerDriver driver, ClusterState clusterState) {
        this.configuration = configuration;
        this.callback = callback;
        this.driver = driver;
        this.clusterState = clusterState;
        clusterState.getTaskList().forEach(this::monitorTask); // Get all previous executors and start monitoring them.
    }

    /**
     * Monitor a new, or existing task.
     * @param task The task to monitor
     */
    public void monitorTask(Protos.TaskInfo task) {
        ESTaskStatus taskStatus = addNewTaskToCluster(task);
        createNewExecutorBump(taskStatus);
        createNewExecutorHealthMonitor(callback, taskStatus);
    }

    // TODO (pnw): Cluster state is not responsibility of cluster monitor.
    public ClusterState getClusterState() {
        return clusterState;
    }

    // TODO (pnw): These will continue for ever. Even if the executor has died.
    private void createNewExecutorHealthMonitor(Scheduler scheduler, ESTaskStatus taskStatus) {
        ExecutorHealth health = new ExecutorHealth(scheduler, driver, taskStatus, 10000L);
        ExecutorHealthCheck healthCheck = new ExecutorHealthCheck(new PollService(health, 5000L));
        pollList.put(taskStatus.getTaskInfo(), healthCheck);
    }

    // TODO (pnw): These will continue for ever. Even if the executor has died.
    private void createNewExecutorBump(ESTaskStatus taskStatus) {
        Protos.TaskInfo taskInfo = taskStatus.getTaskInfo();
        BumpExecutor bumpExecutor = new BumpExecutor(driver, taskInfo);
        ExecutorHealthCheck healthCheck = new ExecutorHealthCheck(new PollService(bumpExecutor, 5000L));
        pollList.put(taskInfo, healthCheck);
    }

    private ESTaskStatus addNewTaskToCluster(Protos.TaskInfo taskInfo) {
        ESTaskStatus taskStatus = new ESTaskStatus(configuration.getState(), configuration.getFrameworkId(), taskInfo);
        taskStatus.setStatus(taskStatus.getDefaultStatus()); // This is a new task, so set default state until we get an update
        clusterState.addTask(taskInfo);
        return taskStatus;
    }

    private void stopPollTask(Protos.TaskInfo taskInfo) {
        pollList.getCollection(taskInfo).forEach(ExecutorHealthCheck::stopHealthcheck);
    }

    public void updateTask(Protos.TaskStatus status) {
        try {
            // Update cluster state, if necessary
            if (getClusterState().exists(status.getTaskId())) {
                LOGGER.debug("Updating task status for: " + status.getTaskId());
                ESTaskStatus executorState = getClusterState().getStatus(status.getTaskId());
                // Update state of Executor
                executorState.setStatus(status);
                // If task in error
                if (executorState.taskInError()) {
                    LOGGER.error("Task in error state. Performing reconciliation: " + executorState.toString());
                    this.stopPollTask(executorState.getTaskInfo()); // Stop polling
                    clusterState.removeTask(executorState.getTaskInfo()); // Remove task from cluster state.
                }
            } else {
                LOGGER.warn("Could not find task in cluster state.");
            }
        } catch (IllegalStateException e) {
            LOGGER.error("Unable to write executor state to zookeeper", e);
        }
    }

    @Override
    public void update(Observable o, Object arg) {
        try {
            this.updateTask((Protos.TaskStatus) arg);
        } catch (ClassCastException e) {
            LOGGER.warn("Received update message, but it was not of type TaskStatus", e);
        }
    }
}
