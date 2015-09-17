package org.apache.mesos.elasticsearch.scheduler.healthcheck;

import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.elasticsearch.scheduler.Configuration;
import org.apache.mesos.elasticsearch.scheduler.state.ESTaskStatus;

import java.util.ArrayList;
import java.util.List;

/**
 * A wrapper class to initiate an asynchronous ping and periodic check.
 */
public class AsyncPing {

    private final Scheduler scheduler;
    private final SchedulerDriver schedulerDriver;
    private final Configuration configuration;
    private final ESTaskStatus taskStatus;
    private final List<ExecutorHealthCheck> healthChecks = new ArrayList<>(2);

    public AsyncPing(Scheduler scheduler, SchedulerDriver schedulerDriver, Configuration configuration, ESTaskStatus taskStatus) {
        this.scheduler = scheduler;
        this.schedulerDriver = schedulerDriver;
        this.configuration = configuration;
        this.taskStatus = taskStatus;
        healthChecks.add(createNewExecutorHealthMonitor());
        healthChecks.add(createNewExecutorBump());
    }

    public void stop() {
        healthChecks.forEach(ExecutorHealthCheck::stopHealthcheck);
    }

    private ExecutorHealthCheck createNewExecutorHealthMonitor() {
        ExecutorHealth health = new ExecutorHealth(scheduler, schedulerDriver, taskStatus, configuration.getExecutorTimeout());
        Long updateRate = configuration.getExecutorHealthDelay() / 2; // Make sure we check more often than the ping.
        return new ExecutorHealthCheck(new PollService(health, updateRate));
    }

    private ExecutorHealthCheck createNewExecutorBump() {
        Protos.TaskInfo taskInfo = taskStatus.getTaskInfo();
        BumpExecutor bumpExecutor = new BumpExecutor(schedulerDriver, taskInfo);
        return new ExecutorHealthCheck(new PollService(bumpExecutor, configuration.getExecutorHealthDelay()));
    }
}
