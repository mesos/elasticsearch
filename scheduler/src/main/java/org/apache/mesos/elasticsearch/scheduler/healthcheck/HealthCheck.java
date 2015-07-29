package org.apache.mesos.elasticsearch.scheduler.healthcheck;

import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;

/**
 * Health check implementation
 */
public class HealthCheck implements Runnable {
    private SchedulerDriver driver;
    private Protos.ExecutorID executorID;
    private Protos.SlaveID slaveID;

    public HealthCheck(SchedulerDriver driver, Protos.SlaveID slaveID, Protos.ExecutorID executorID) {
        this.driver = driver;
        this.executorID = executorID;
        this.slaveID = slaveID;
    }

    @Override
    public void run() {
        driver.sendFrameworkMessage(executorID, slaveID, Protos.HealthCheck.newBuilder().build().toByteArray());
    }
}
