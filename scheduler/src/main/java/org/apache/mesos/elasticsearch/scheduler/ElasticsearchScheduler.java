package org.apache.mesos.elasticsearch.scheduler;

import org.apache.log4j.Logger;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.elasticsearch.common.SerializableIPAddress;
import org.apache.mesos.elasticsearch.scheduler.state.*;

import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Scheduler for Elasticsearch.
 */
@SuppressWarnings({"PMD.TooManyMethods"})
public class ElasticsearchScheduler implements Scheduler {

    private static final Logger LOGGER = Logger.getLogger(ElasticsearchScheduler.class.toString());

    private final Configuration configuration;
    private final TaskInfoFactory taskInfoFactory;
    private final CredentialFactory credentialFactory;
    private final ClusterState clusterState;
    private FrameworkState frameworkState;
    private OfferStrategy offerStrategy;
    private SerializableState zookeeperStateDriver;

    public ElasticsearchScheduler(Configuration configuration, FrameworkState frameworkState, ClusterState clusterState, TaskInfoFactory taskInfoFactory, OfferStrategy offerStrategy, SerializableState zookeeperStateDriver) {
        this.configuration = configuration;
        this.frameworkState = frameworkState;
        this.clusterState = clusterState;
        this.taskInfoFactory = taskInfoFactory;
        this.offerStrategy = offerStrategy;
        this.zookeeperStateDriver = zookeeperStateDriver;
        this.credentialFactory = new CredentialFactory(configuration);
    }

    public Map<String, Task> getTasks() {
        if (clusterState == null) {
            return new HashMap<>();
        } else {
            return clusterState.getGuiTaskList();
        }
    }

    public void run() {
        LOGGER.info("Starting ElasticSearch on Mesos - [numHwNodes: " + configuration.getElasticsearchNodes() +
                ", zk mesos: " + configuration.getMesosZKURL() +
                ", zk framework: " + configuration.getFrameworkZKURL() +
                ", ram:" + configuration.getMem() + "]");

        FrameworkInfoFactory frameworkInfoFactory = new FrameworkInfoFactory(configuration, frameworkState);
        final Protos.FrameworkInfo.Builder frameworkBuilder = frameworkInfoFactory.getBuilder();
        final Protos.Credential.Builder credentialBuilder = credentialFactory.getBuilder();
        final MesosSchedulerDriver driver;
        if (credentialBuilder.isInitialized()) {
            LOGGER.debug("Creating Scheduler driver with principal: " + credentialBuilder.toString());
            driver = new MesosSchedulerDriver(this, frameworkBuilder.build(), configuration.getMesosZKURL(), credentialBuilder.build());
        } else {
            driver = new MesosSchedulerDriver(this, frameworkBuilder.build(), configuration.getMesosZKURL());
        }

        driver.run();
    }

    @Override
    public void registered(SchedulerDriver driver, Protos.FrameworkID frameworkId, Protos.MasterInfo masterInfo) {
        LOGGER.info("Framework registered as " + frameworkId.getValue());

        List<Protos.Resource> resources = Resources.buildFrameworkResources(configuration);

        Protos.Request request = Protos.Request.newBuilder()
                .addAllResources(resources)
                .build();

        List<Protos.Request> requests = Collections.singletonList(request);
        driver.requestResources(requests);

        frameworkState.markRegistered(frameworkId, driver);
    }

    @Override
    public void reregistered(SchedulerDriver driver, Protos.MasterInfo masterInfo) {
        LOGGER.info("Framework re-registered");
    }

    @Override
    public void resourceOffers(SchedulerDriver driver, List<Protos.Offer> offers) {
        if (!frameworkState.isRegistered()) {
            LOGGER.debug("Not registered, can't accept resource offers.");
            return;
        }
        for (Protos.Offer offer : offers) {
            final OfferStrategy.OfferResult result = offerStrategy.evaluate(offer);

            if (!result.acceptable) {
                LOGGER.debug("Declined offer: " + result.reason.orElse("Unknown"));
                driver.declineOffer(offer.getId());
            } else {
                Protos.TaskInfo taskInfo = taskInfoFactory.createTask(configuration, frameworkState, offer);
                LOGGER.debug(taskInfo.toString());
                driver.launchTasks(Collections.singleton(offer.getId()), Collections.singleton(taskInfo));
                ESTaskStatus esTask = new ESTaskStatus(zookeeperStateDriver, frameworkState.getFrameworkID(), taskInfo, new StatePath(zookeeperStateDriver)); // Write staging state to zk
                clusterState.addTask(esTask); // Add tasks to cluster state and write to zk
                frameworkState.announceNewTask(esTask);
            }
        }
    }

    @Override
    public void offerRescinded(SchedulerDriver driver, Protos.OfferID offerId) {
        LOGGER.info("Offer " + offerId.getValue() + " rescinded");
    }

    @Override
    public void statusUpdate(SchedulerDriver driver, Protos.TaskStatus status) {
        LOGGER.info("Status update - Task with ID '" + status.getTaskId().getValue() + "' is now in state '" + status.getState() + "'. Message: " + status.getMessage());
        frameworkState.announceStatusUpdate(status);
    }

    @Override
    public void frameworkMessage(SchedulerDriver driver, Protos.ExecutorID executorId, Protos.SlaveID slaveId, byte[] data) {
        LOGGER.info("Framework Message - Executor: " + executorId.getValue() + ", SlaveID: " + slaveId.getValue());
        try {
            SerializableIPAddress serializableIPAddress = SerializableIPAddress.fromBytes(data);
            Protos.TaskInfo oldTask = clusterState.getTask(executorId);
            Protos.TaskInfo newTask = Protos.TaskInfo.newBuilder().mergeFrom(oldTask)
                    .setData(taskInfoFactory.toData(serializableIPAddress.getAddress().getHostName(), serializableIPAddress.getAddress().getHostAddress(), new Clock().zonedNow()))
                    .build();
            clusterState.removeTask(oldTask);
            clusterState.addTask(newTask);
        } catch (UnknownHostException e) {
            LOGGER.warn("Unable to parse ip address discovery information");
        }
    }

    @Override
    public void disconnected(SchedulerDriver driver) {
        LOGGER.warn("Disconnected");
    }

    @Override
    public void slaveLost(SchedulerDriver driver, Protos.SlaveID slaveId) {
        LOGGER.info("Slave lost: " + slaveId.getValue());
    }

    @Override
    public void executorLost(SchedulerDriver driver, Protos.ExecutorID executorId, Protos.SlaveID slaveId, int status) {
        // This is never called by Mesos, so we have to call it ourselves via a healthcheck
        // https://issues.apache.org/jira/browse/MESOS-313
        LOGGER.info("Executor lost: " + executorId.getValue() +
                " on slave " + slaveId.getValue() +
                " with status " + status);
        try {
            Protos.TaskInfo taskInfo = clusterState.getTask(executorId);
            statusUpdate(driver, Protos.TaskStatus.newBuilder().setExecutorId(executorId).setSlaveId(slaveId).setTaskId(taskInfo.getTaskId()).setState(Protos.TaskState.TASK_LOST).build());
            driver.killTask(taskInfo.getTaskId()); // It may not actually be lost, it may just have hanged. So Kill, just in case.
        } catch (IllegalArgumentException e) {
            LOGGER.warn("Unable to find TaskInfo with the given Executor ID", e);
        }
    }

    @Override
    public void error(SchedulerDriver driver, String message) {
        LOGGER.error("Error: " + message);
    }
}
