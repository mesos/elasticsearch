package org.apache.mesos.elasticsearch.scheduler.cluster;

import org.apache.log4j.Logger;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.elasticsearch.scheduler.Configuration;
import org.apache.mesos.elasticsearch.scheduler.healthcheck.AsyncPing;
import org.apache.mesos.elasticsearch.scheduler.state.ClusterState;
import org.apache.mesos.elasticsearch.scheduler.state.ESTaskStatus;

import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.Map;
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
    private final Map<Protos.TaskInfo, AsyncPing> healthChecks = new HashMap<>();

    public ClusterMonitor(Configuration configuration, Scheduler callback, SchedulerDriver driver, ClusterState clusterState) {
        if (configuration == null || callback == null || driver == null || clusterState == null) {
            throw new InvalidParameterException("Constructor parameters cannot be null.");
        }
        this.configuration = configuration;
        this.callback = callback;
        this.driver = driver;
        this.clusterState = clusterState;
        clusterState.getTaskList().forEach(this::startMonitoringTask); // Get all previous executors and start monitoring them.
    }

    /**
     * Start monitoring a task
     * @param taskInfo The task to monitor
     */
    public void startMonitoringTask(Protos.TaskInfo taskInfo) {
        LOGGER.debug("Start monitoring: " + taskInfo.getTaskId().getValue());
        healthChecks.put(taskInfo, new AsyncPing(callback, driver, configuration, new ESTaskStatus(configuration.getState(), configuration.getFrameworkId(), taskInfo)));
    }

    private void stopMonitoringTask(Protos.TaskInfo taskInfo) {
        LOGGER.debug("Stop monitoring: " + taskInfo.getTaskId().getValue());
        healthChecks.remove(taskInfo).stop(); // Remove task from list and stop its healthchecks.
    }

    /**
     * Updates a task with the given status. Status is written to zookeeper.
     * If the task is in error, then the healthchecks are stopped and state is removed from ZK
     * @param status A received task status
     */
    private void updateTask(Protos.TaskStatus status) {
        if (clusterState.exists(status.getTaskId())) {
            LOGGER.warn("Could not find task in cluster state.");
            return;
        }

        try {
            Protos.TaskInfo taskInfo = clusterState.getTask(status.getTaskId());
            LOGGER.debug("Updating task status for executor: " + status.getExecutorId().getValue() + " [" + status.getTaskId().getValue() + ", " + status.getTimestamp() + ", " + status.getState() + "]");
            clusterState.update(status); // Update state of Executor

            if (clusterState.taskInError(status)) {
                LOGGER.error("Task in error state. Removing state for executor: " + status.getExecutorId().getValue() + ", due to: " + status.getState());
                stopMonitoringTask(taskInfo);
                clusterState.removeTask(taskInfo); // Remove task from cluster state.
            }
        } catch (IllegalStateException | IllegalArgumentException e) {
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

    public Map<Protos.TaskInfo, AsyncPing> getHealthChecks() {
        return healthChecks;
    }
}
