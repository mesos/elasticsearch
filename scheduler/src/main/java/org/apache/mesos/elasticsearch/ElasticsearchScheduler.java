package org.apache.mesos.elasticsearch;

import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import static java.lang.String.format;
import static java.util.Arrays.asList;

/**
 * Scheduler for Elasticsearch.
 */
public class ElasticsearchScheduler implements Scheduler {

    public static final Logger LOGGER = Logger.getLogger(ElasticsearchScheduler.class.toString());

    public static final String TASK_DATE_FORMAT = "yyyyMMdd'T'HHmmss.SSS'Z'";

    Clock clock = new Clock();

    Set<Task> tasks = new HashSet<>();

    private int numberOfHwNodes;

    public ElasticsearchScheduler(int numberOfHwNodes) {
        this.numberOfHwNodes = numberOfHwNodes;
    }

    @Override
    public void registered(SchedulerDriver driver, Protos.FrameworkID frameworkId, Protos.MasterInfo masterInfo) {
        LOGGER.info("Framework registered as " + frameworkId.getValue());

        Protos.Resource cpuResource = Protos.Resource.newBuilder()
                .setName("cpus")
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(1.0).build())
                .build();

        Protos.Request request = Protos.Request.newBuilder()
                .addResources(cpuResource)
                .build();

        List<Protos.Request> requests = Collections.singletonList(request);
        driver.requestResources(requests);
    }

    @Override
    public void reregistered(SchedulerDriver driver, Protos.MasterInfo masterInfo) {
        LOGGER.info("Framework re-registered");
    }

    @Override
    public void resourceOffers(SchedulerDriver driver, List<Protos.Offer> offers) {
        Protos.Resource cpus = Protos.Resource.newBuilder()
                .setName("cpus")
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(1.0).build())
                .build();

        Protos.Resource mem = Protos.Resource.newBuilder()
                .setName("mem")
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(2048).build())
                .build();

        Protos.Resource disk = Protos.Resource.newBuilder()
                .setName("disk")
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(1000).build())
                .build();

        List<Protos.Resource> resources = asList(cpus, mem, disk);

        for (Protos.Offer offer : offers) {
            if (isOfferGood(offer) && !haveEnoughNodes()) {
                LOGGER.fine("Offer: " + offer);

                LOGGER.info("Accepted offer: " + offer.getHostname());

                String id = taskId(offer);

                Protos.ContainerInfo.DockerInfo docker = Protos.ContainerInfo.DockerInfo.newBuilder()
                        .setImage("mesos/elasticsearch-node").build();

                Protos.ContainerInfo.Builder container = Protos.ContainerInfo.newBuilder()
                        .setDocker(docker)
                        .setType(Protos.ContainerInfo.Type.DOCKER);

                Protos.TaskInfo taskInfo = Protos.TaskInfo.newBuilder()
                        .setName(id)
                        .setTaskId(Protos.TaskID.newBuilder().setValue(id))
                        .setSlaveId(offer.getSlaveId())
                        .addAllResources(resources)
                        .setContainer(container)
                        .setCommand(Protos.CommandInfo.newBuilder()
                                .addArguments("elasticsearch")
                                .addArguments("--logger.discovery").addArguments("INFO")
                                .setShell(false))
                        .build();

                driver.launchTasks(Collections.singleton(offer.getId()), Collections.singleton(taskInfo));
                tasks.add(new Task(offer.getHostname(), id));
            } else {
                driver.declineOffer(offer.getId());
                LOGGER.info("Declined offer: " + offer.getHostname());
            }
        }
    }

    @Override
    public void offerRescinded(SchedulerDriver driver, Protos.OfferID offerId) {
        LOGGER.warning("Offer " + offerId.getValue() + " rescinded");
    }

    @Override
    public void statusUpdate(SchedulerDriver driver, Protos.TaskStatus status) {
        LOGGER.fine("Status update - Task ID: " + status.getTaskId() + ", State: " + status.getState());
    }

    @Override
    public void frameworkMessage(SchedulerDriver driver, Protos.ExecutorID executorId, Protos.SlaveID slaveId, byte[] data) {
        LOGGER.fine("Framework Message - Executor: " + executorId.getValue() + ", SlaveID: " + slaveId.getValue());
    }

    @Override
    public void disconnected(SchedulerDriver driver) {
        LOGGER.warning("Disconnected");
    }

    @Override
    public void slaveLost(SchedulerDriver driver, Protos.SlaveID slaveId) {
        LOGGER.warning("Slave lost: " + slaveId.getValue());
    }

    @Override
    public void executorLost(SchedulerDriver driver, Protos.ExecutorID executorId, Protos.SlaveID slaveId, int status) {
        LOGGER.warning("Executor lost: " + executorId.getValue() +
                "on slave " + slaveId.getValue() +
                "with status " + status);
    }

    @Override
    public void error(SchedulerDriver driver, String message) {
        LOGGER.severe("Error: " + message);
    }

    private String taskId(Protos.Offer offer) {
        String date = new SimpleDateFormat(TASK_DATE_FORMAT).format(clock.now());
        return format("elasticsearch_%s_%s", offer.getHostname(), date);
    }

    private boolean isOfferGood(Protos.Offer offer) {
        // Don't start the same framework multiple times on the same host
        for (Task task : tasks) {
            if (task.getHostname().equals(offer.getHostname())) {
                return false;
            }
        }
        return true;
    }

    private boolean haveEnoughNodes() {
        return tasks.size() == numberOfHwNodes;
    }

}
