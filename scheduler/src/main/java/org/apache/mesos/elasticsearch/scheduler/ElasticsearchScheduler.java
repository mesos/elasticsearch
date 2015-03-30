package org.apache.mesos.elasticsearch.scheduler;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

/**
 * Scheduler for Elasticsearch.
 */
@SuppressWarnings("PMD.TooManyMethods")
public class ElasticsearchScheduler implements Scheduler, Runnable {

    public static final Logger LOGGER = Logger.getLogger(ElasticsearchScheduler.class.toString());

    public static final String TASK_DATE_FORMAT = "yyyyMMdd'T'HHmmss.SSS'Z'";

    Clock clock = new Clock();

    Set<Task> tasks = new HashSet<>();

    private CountDownLatch initialized = new CountDownLatch(1);

    private int numberOfHwNodes;

    private String masterUrl;

    public ElasticsearchScheduler(String masterUrl, int numberOfHwNodes) {
        this.masterUrl = masterUrl;
        this.numberOfHwNodes = numberOfHwNodes;
    }

    public static void main(String[] args) {
        Options options = new Options();
        options.addOption("m", "masterUrl", true, "master url");
        options.addOption("n", "numHardwareNodes", true, "number of hardware nodes");
        CommandLineParser parser = new BasicParser();
        try {
            CommandLine cmd = parser.parse(options, args);
            String masterUrl = cmd.getOptionValue("m");
            String numberOfHwNodesString = cmd.getOptionValue("n");
            if (masterUrl == null || numberOfHwNodesString == null) {
                printUsage(options);
                return;
            }
            int numberOfHwNodes = 1;
            try {
                numberOfHwNodes = Integer.parseInt(numberOfHwNodesString);
            } catch (IllegalArgumentException e) {
                printUsage(options);
                return;
            }

            LOGGER.info("Starting ElasticSearch on Mesos - [master: " + masterUrl + ", numHwNodes: " + numberOfHwNodes + "]");

            ElasticsearchScheduler scheduler = new ElasticsearchScheduler(masterUrl, numberOfHwNodes);

            Thread schedThred = new Thread(scheduler);
            schedThred.start();
            scheduler.waitUntilInit();

            Set<Task> tasks = scheduler.getTasks();
            List<String> nodes = new ArrayList<>();
            for (Task task : tasks) {
                nodes.add(task.getHostname());
            }

            LOGGER.info("ElasticSearch nodes starting on: " + nodes);
        } catch (ParseException e) {
            printUsage(options);
        }
    }

    private void waitUntilInit() {
        try {
            initialized.await();
        } catch (InterruptedException e) {
            LOGGER.severe("Elasticsearch framework interrupted");
        }
    }

    private static void printUsage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("elasticsearch-scheduler", options);
    }

    @Override
    public void run() {
        LOGGER.info("Starting up...");
        SchedulerDriver driver = new MesosSchedulerDriver(this, Protos.FrameworkInfo.newBuilder().setUser("").setName("ElasticSearch").build(), masterUrl);
        driver.run();
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

        List<Protos.Resource> resources = Arrays.asList(cpus, mem, disk);

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

        if (haveEnoughNodes()) {
            initialized.countDown();
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
        return String.format("elasticsearch_%s_%s", offer.getHostname(), date);
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

    public Set<Task> getTasks() {
        return tasks;
    }
}
