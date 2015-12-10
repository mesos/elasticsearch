package org.apache.mesos.elasticsearch.scheduler.cluster;

import org.apache.log4j.Logger;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.elasticsearch.scheduler.Configuration;
import org.apache.mesos.elasticsearch.scheduler.state.ClusterState;

import java.util.List;

/**
 * A scheduled task to reap tasks when the configuration changes
 */
public class TaskReaper implements Runnable {
    private static final Logger LOGGER = Logger.getLogger(TaskReaper.class);

    private ClusterState clusterState;
    private Configuration configuration;
    private SchedulerDriver schedulerDriver;

    public TaskReaper(SchedulerDriver schedulerDriver, Configuration configuration, ClusterState clusterState) {
        this.clusterState = clusterState;
        this.configuration = configuration;
        this.schedulerDriver = schedulerDriver;
        if (clusterState == null || configuration == null) {
            throw new IllegalArgumentException("Task reaper cannot start with null cluster state or configuration");
        }
    }

    @Override
    public void run() {
        try {
            int numToKill = clusterState.getTaskList().size() - configuration.getElasticsearchNodes();
            if (numToKill > 0) {
                LOGGER.debug("Task reaper. There are " + numToKill + " tasks to kill");
                List<Protos.TaskInfo> taskList = clusterState.getTaskList();
                int size = taskList.size();
                numToKill = Math.min(numToKill, size);
                for (int x = 0; x < numToKill; x++) {
                    int taskIndex = size - 1 - x;
                    LOGGER.debug("Killing task index " + taskIndex);
                    Protos.TaskInfo killTaskInfo = taskList.get(taskIndex);
                    Protos.TaskID killTaskId = killTaskInfo.getTaskId();
                    LOGGER.debug("Killing task: " + killTaskId);
                    Protos.Status status = schedulerDriver.killTask(killTaskId);
                    LOGGER.debug("Kill request response: " + status.toString());
                }
            }
        } catch (Exception ex) { // One hell of a catch all. ScheduledExecutorService not restart if there is an exception. So we catch all to prevent it from silently failing.
            LOGGER.debug("Problem with the task reaper, but carrying on like a trooper.", ex);
        }
    }
}
