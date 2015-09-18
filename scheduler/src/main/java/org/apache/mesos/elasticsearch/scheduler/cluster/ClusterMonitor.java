package org.apache.mesos.elasticsearch.scheduler.cluster;

import org.apache.log4j.Logger;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.elasticsearch.scheduler.Configuration;
import org.apache.mesos.elasticsearch.scheduler.healthcheck.AsyncPing;
import org.apache.mesos.elasticsearch.scheduler.state.ESTaskStatus;
import org.apache.mesos.elasticsearch.scheduler.state.StatePath;

import java.security.InvalidParameterException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Contains all cluster information. Monitors state of cluster elements.
 */
public class ClusterMonitor implements Observer {
    private static final Logger LOGGER = Logger.getLogger(ClusterMonitor.class);
    private final Configuration configuration;
    private final Scheduler callback;
    private final SchedulerDriver driver;
    private final Map<Protos.TaskInfo, AsyncPing> healthChecks = new HashMap<>();
    private final StatePath statePath;

    public ClusterMonitor(Configuration configuration, Scheduler callback, SchedulerDriver driver, StatePath statePath) {
        if (configuration == null || callback == null || driver == null || statePath == null) {
            throw new InvalidParameterException("Constructor parameters cannot be null.");
        }
        this.configuration = configuration;
        this.callback = callback;
        this.driver = driver;
        this.statePath = statePath;
    }

    public void startMonitoringTask(ESTaskStatus esTask) {
        startMonitoringTask(esTask.getTaskInfo());
    }

    /**
     * Start monitoring a task
     * @param taskInfo The task to monitor
     */
    public void startMonitoringTask(Protos.TaskInfo taskInfo) {
        LOGGER.debug("Start monitoring: " + taskInfo.getTaskId().getValue());
        healthChecks.put(taskInfo, new AsyncPing(callback, driver, configuration, new ESTaskStatus(configuration.getState(), configuration.getFrameworkId(), taskInfo, statePath)));
    }

    private void stopMonitoringTask(Protos.TaskInfo taskInfo) {
        LOGGER.debug("Stop monitoring: " + taskInfo.getTaskId().getValue());
        healthChecks.remove(taskInfo).stop(); // Remove task from list and stop its healthchecks.
    }

    private List<Protos.TaskID> getTaskIDList() {
        return healthChecks.keySet().stream().map(Protos.TaskInfo::getTaskId).collect(Collectors.toList());
    }

    private Protos.TaskInfo getTaskInfo(Protos.TaskID taskID) {
        return healthChecks.keySet().stream().filter(taskInfo -> taskInfo.getTaskId().equals(taskID)).findFirst().get();
    }

    /**
     * Updates a task with the given status. Status is written to zookeeper.
     * If the task is in error, then the healthchecks are stopped and state is removed from ZK
     * @param status A received task status
     */
    private void updateTask(Protos.TaskStatus status) {
        if (!getTaskIDList().contains(status.getTaskId())) {
            LOGGER.warn("Could not find task in monitor list.");
            return;
        }

        try {
            if (ESTaskStatus.errorState(status.getState())) {
                LOGGER.error("Task in error state. Removing executor from monitor list: " + status.getExecutorId().getValue() + ", due to: " + status.getState());
                stopMonitoringTask(getTaskInfo(status.getTaskId()));
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
