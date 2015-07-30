package org.apache.mesos.elasticsearch.scheduler.healthcheck;

import org.apache.log4j.Logger;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.HealthCheck;
import org.apache.mesos.SchedulerDriver;

/**
 * Health check implementation
 */
public class BumpExecutor implements Runnable {
    private static final Logger LOGGER = Logger.getLogger(BumpExecutor.class);
    private final SchedulerDriver driver;
    private final TaskInfo taskInfo;

    public BumpExecutor(SchedulerDriver driver, TaskInfo taskInfo) {
        this.driver = driver;
        this.taskInfo = taskInfo;
    }

    @Override
    public void run() {
        driver.sendFrameworkMessage(taskInfo.getExecutor().getExecutorId(), taskInfo.getSlaveId(), HealthCheck.newBuilder().build().toByteArray());
    }
}
