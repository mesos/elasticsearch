package org.apache.mesos.elasticsearch.scheduler.healthcheck;

import org.apache.log4j.Logger;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.elasticsearch.scheduler.state.ESTaskStatus;

import java.security.InvalidParameterException;

/**
 * Checks Zookeeper for the last udpate. If it is greater than a timeout, executor lost is called.
 */
public class ExecutorHealth implements Runnable {
    private static final Logger LOGGER = Logger.getLogger(ExecutorHealth.class);
    public static final int EXIT_STATUS = 1;
    private final Scheduler scheduler;
    private final SchedulerDriver driver;
    private final ESTaskStatus taskStatus;
    private final Long maxTimeout;
    private Double lastUpdate = Double.MAX_VALUE;

    public ExecutorHealth(Scheduler scheduler, SchedulerDriver driver, ESTaskStatus taskStatus, Long maxTimeout) {
        if (scheduler == null) {
            throw new InvalidParameterException("Scheduler cannot be null.");
        } else if (driver == null) {
            throw new InvalidParameterException("Scheduler driver cannot be null.");
        } else if (taskStatus == null || taskStatus.getStatus() == null) {
            throw new InvalidParameterException("Task status cannot be null.");
        } else if (maxTimeout <= 0) {
            throw new InvalidParameterException("Max timeout cannot be less than or equal to zero.");
        }
        this.scheduler = scheduler;
        this.driver = driver;
        this.taskStatus = taskStatus;
        this.maxTimeout = maxTimeout;
    }

    @Override
    public void run() {
        try {
            Double thisUpdate = taskStatus.getStatus().getTimestamp();
            Double timeSinceUpdate = thisUpdate - lastUpdate;
            if (timeSinceUpdate > maxTimeout) {
                scheduler.executorLost(driver, taskStatus.getStatus().getExecutorId(), taskStatus.getStatus().getSlaveId(), EXIT_STATUS);
            }
            lastUpdate = thisUpdate;
        } catch (Exception e) {
            LOGGER.error("Unable to read executor health", e);
        }
    }

    public Double getLastUpdate() {
        return lastUpdate;
    }
}
