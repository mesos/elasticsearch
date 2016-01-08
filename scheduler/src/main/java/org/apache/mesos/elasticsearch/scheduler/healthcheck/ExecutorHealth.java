package org.apache.mesos.elasticsearch.scheduler.healthcheck;

import org.apache.log4j.Logger;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.elasticsearch.scheduler.state.ESTaskStatus;

import java.security.InvalidParameterException;

/**
 * Checks Zookeeper for the last udpate. If it is greater than a timeout, executor lost is called.
 */
public class ExecutorHealth implements Runnable {
    public static final int EXIT_STATUS = 1;
    public static final double EPSILON = 1E-12;
    private static final Logger LOGGER = Logger.getLogger(ExecutorHealth.class);
    private final Scheduler scheduler;
    private final SchedulerDriver driver;
    private final ESTaskStatus taskStatus;
    private final Long maxTimeout;
    private Long lastUpdate = Long.MAX_VALUE;

    public ExecutorHealth(Scheduler scheduler, SchedulerDriver driver, ESTaskStatus taskStatus, Long maxTimeoutMS) {
        if (scheduler == null) {
            throw new InvalidParameterException("Scheduler cannot be null.");
        } else if (driver == null) {
            throw new InvalidParameterException("Scheduler driver cannot be null.");
        } else if (taskStatus == null) {
            throw new InvalidParameterException("Task status cannot be null.");
        } else if (maxTimeoutMS <= 0) {
            throw new InvalidParameterException("Max timeout cannot be less than or equal to zero.");
        }
        this.scheduler = scheduler;
        this.driver = driver;
        this.taskStatus = taskStatus;
        this.maxTimeout = maxTimeoutMS;
    }

    @Override
    public void run() {
        try {
            Protos.TaskStatus status = taskStatus.getStatus();
            Double thisUpdate = status.getTimestamp(); // Mesos timestamps in seconds, a double.
            if (Math.abs(thisUpdate) < EPSILON) {
                LOGGER.info("Ignoring status update timestamp. Was zero: " + thisUpdate);
                return;
            }
            Long thisUpdateMs = (long) (thisUpdate * 1000.0);
            Long timeSinceUpdate = thisUpdateMs - lastUpdate;
            if (timeSinceUpdate > maxTimeout) {
                Protos.ExecutorID executorId = null;
                if (taskStatus.getTaskInfo() != null && taskStatus.getTaskInfo().getExecutor() != null) {
                    executorId = taskStatus.getTaskInfo().getExecutor().getExecutorId();
                }
                LOGGER.warn("Executor " + ((executorId != null) ? executorId.getValue() : null) +
                        " is not responding to healthchecks in required timeout (" + maxTimeout + " ms). " +
                        "It has been " + timeSinceUpdate + " ms since the last update. (" + thisUpdateMs + " - " + lastUpdate + ")");
                scheduler.executorLost(driver, executorId, status.getSlaveId(), EXIT_STATUS);
            } else {
                LOGGER.debug("Task is alive. It has been " + timeSinceUpdate + " ms since the last update.");
                lastUpdate = thisUpdateMs;
            }
        } catch (Exception e) {
            LOGGER.error("Unable to read executor health", e);
        }
    }

    public Long getLastUpdate() {
        return lastUpdate;
    }
}
