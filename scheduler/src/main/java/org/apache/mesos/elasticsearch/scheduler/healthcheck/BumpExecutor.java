package org.apache.mesos.elasticsearch.scheduler.healthcheck;

import org.apache.log4j.Logger;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;

import java.util.Collections;

/**
 * Health check implementation
 */
public class BumpExecutor implements Runnable {
    private static final Logger LOGGER = Logger.getLogger(BumpExecutor.class);
    private final SchedulerDriver driver;
    private final Protos.TaskStatus taskStatus;

    public BumpExecutor(SchedulerDriver driver, Protos.TaskStatus taskStatus) {
        this.driver = driver;
        this.taskStatus = taskStatus;
    }

    @Override
    public void run() {
        driver.reconcileTasks(Collections.singletonList(taskStatus));
    }
}
