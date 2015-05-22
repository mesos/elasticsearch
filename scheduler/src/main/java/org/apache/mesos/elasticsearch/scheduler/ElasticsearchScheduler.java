package org.apache.mesos.elasticsearch.scheduler;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.elasticsearch.common.Binaries;
import org.apache.mesos.elasticsearch.common.Configuration;
import org.elasticsearch.common.collect.Lists;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * Scheduler for Elasticsearch.
 */
@SuppressWarnings("PMD.TooManyMethods")
public class ElasticsearchScheduler implements Scheduler, Runnable {

    public static final Logger LOGGER = Logger.getLogger(ElasticsearchScheduler.class.toString());

    public static final String TASK_DATE_FORMAT = "yyyyMMdd'T'HHmmss.SSS'Z'";

    // As per the DCOS Service Specification, setting the failover timeout to a large value;
    private static final double FAILOVER_TIMEOUT = 86400000;

    Clock clock = new Clock();

    Set<Task> tasks = new HashSet<>();

    private CountDownLatch initialized = new CountDownLatch(1);

    private int numberOfHwNodes;

    private String masterUrl;

    private boolean useDocker;

    private String namenode;

    private Protos.FrameworkID frameworkId;

    public ElasticsearchScheduler(String masterUrl, int numberOfHwNodes, boolean useDocker, String namenode) {
        this.masterUrl = masterUrl;
        this.numberOfHwNodes = numberOfHwNodes;
        this.useDocker = useDocker;
        this.namenode = namenode;
    }

    public static void main(String[] args) {
        Options options = new Options();
        options.addOption("m", "masterUrl", true, "master url");
        options.addOption("n", "numHardwareNodes", true, "number of hardware nodes");
        options.addOption("d", "useDocker", false, "use docker to launch Elasticsearch");
        options.addOption("nn", "namenode", true, "name node hostname + port");
        CommandLineParser parser = new BasicParser();
        try {
            CommandLine cmd = parser.parse(options, args);
            String masterUrl = cmd.getOptionValue("m");
            String numberOfHwNodesString = cmd.getOptionValue("n");
            String nameNode = cmd.getOptionValue("nn");
            if (masterUrl == null || numberOfHwNodesString == null || nameNode == null) {
                printUsage(options);
                return;
            }
            int numberOfHwNodes;
            try {
                numberOfHwNodes = Integer.parseInt(numberOfHwNodesString);
            } catch (IllegalArgumentException e) {
                printUsage(options);
                return;
            }

            boolean useDocker = cmd.hasOption('d');

            LOGGER.info("Starting ElasticSearch on Mesos - [master: " + masterUrl + ", numHwNodes: " + numberOfHwNodes + ", docker: " + (useDocker ? "enabled" : "disabled") + "]");

            final ElasticsearchScheduler scheduler = new ElasticsearchScheduler(masterUrl, numberOfHwNodes, useDocker, nameNode);

            final Protos.FrameworkInfo.Builder frameworkBuilder = Protos.FrameworkInfo.newBuilder();
            frameworkBuilder.setUser("jclouds");
            frameworkBuilder.setName(Configuration.FRAMEWORK_NAME);
            frameworkBuilder.setCheckpoint(true);
            frameworkBuilder.setFailoverTimeout(FAILOVER_TIMEOUT);

            final MesosSchedulerDriver driver = new MesosSchedulerDriver(scheduler, frameworkBuilder.build(), masterUrl);

            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                @Override
                public void run() {
                    driver.stop();
                    scheduler.onShutdown();
                }
            }));

            Thread schedThred = new Thread(scheduler);
            schedThred.start();
            scheduler.waitUntilInit();

            final List<String> nodes = Lists.newArrayList(); //.stream().map(Task::getHostname).collect(toList());

            for (Task task : scheduler.getTasks()) {
                nodes.add(task.getHostname());
            }

            LOGGER.info("ElasticSearch nodes starting on: " + nodes);
        } catch (ParseException e) {
            printUsage(options);
        }
    }

    private void onShutdown() {
        LOGGER.info("On shutdown...");
    }

    private void waitUntilInit() {
        try {
            initialized.await();
        } catch (InterruptedException e) {
            LOGGER.error("Elasticsearch framework interrupted");
        }
    }

    private static void printUsage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(Configuration.FRAMEWORK_NAME, options);
    }

    @Override
    public void run() {
        LOGGER.info("Starting up...");
        SchedulerDriver driver = new MesosSchedulerDriver(this, Protos.FrameworkInfo.newBuilder().setUser("").setName(Configuration.FRAMEWORK_NAME).build(), masterUrl);
        driver.run();
    }

    @Override
    public void registered(SchedulerDriver driver, Protos.FrameworkID frameworkId, Protos.MasterInfo masterInfo) {
        this.frameworkId = frameworkId;

        LOGGER.info("Framework registered as " + frameworkId.getValue());

        List<Protos.Resource> resources = buildResources();

        Protos.Request request = Protos.Request.newBuilder()
                .addAllResources(resources)
                .build();

        List<Protos.Request> requests = Collections.singletonList(request);
        driver.requestResources(requests);
    }

    private static List<Protos.Resource> buildResources() {
        Protos.Resource cpus = Protos.Resource.newBuilder()
                .setName("cpus")
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(Configuration.CPUS).build())
                .build();

        Protos.Resource mem = Protos.Resource.newBuilder()
                .setName("mem")
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(Configuration.MEM).build())
                .build();

        Protos.Resource disk = Protos.Resource.newBuilder()
                .setName("disk")
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(Configuration.DISK).build())
                .build();

        Protos.Value.Range clientPortRange = Protos.Value.Range.newBuilder().setBegin(Configuration.ELASTICSEARCH_CLIENT_PORT).setEnd(Configuration.ELASTICSEARCH_CLIENT_PORT).build();
        Protos.Value.Range transportPortRange = Protos.Value.Range.newBuilder().setBegin(Configuration.ELASTICSEARCH_TRANSPORT_PORT).setEnd(Configuration.ELASTICSEARCH_TRANSPORT_PORT).build();

        Protos.Resource ports = Protos.Resource.newBuilder()
                .setName("ports")
                .setType(Protos.Value.Type.RANGES)
                .setRanges(Protos.Value.Ranges.newBuilder().addRange(clientPortRange).addRange(transportPortRange))
                .build();

        return Arrays.asList(cpus, mem, disk, ports);
    }

    @Override
    public void reregistered(SchedulerDriver driver, Protos.MasterInfo masterInfo) {
        LOGGER.info("Framework re-registered");
    }

    @Override
    public void resourceOffers(SchedulerDriver driver, List<Protos.Offer> offers) {
        List<Protos.Resource> resources = buildResources();

        for (Protos.Offer offer : offers) {
            if (isOfferGood(offer) && !haveEnoughNodes()) {
                LOGGER.info("Accepted offer: " + offer.getHostname() + " - " + offer.toString());

                String id = taskId(offer);

                Protos.TaskInfo taskInfo = buildTask(resources, offer, id);

                driver.launchTasks(Collections.singleton(offer.getId()), Collections.singleton(taskInfo));
                tasks.add(new Task(offer.getHostname(), id));
            } else {
                driver.declineOffer(offer.getId());
                LOGGER.info("Declined offer: " + offer.getHostname() + " - " + offer.toString());
            }
        }

        if (haveEnoughNodes()) {
            initialized.countDown();
        }
    }

    private Protos.TaskInfo buildTask(List<Protos.Resource> resources, Protos.Offer offer, String id) {
        Protos.TaskInfo.Builder taskInfoBuilder = Protos.TaskInfo.newBuilder()
                .setName(Configuration.TASK_NAME)
                .setTaskId(Protos.TaskID.newBuilder().setValue(id))
                .setSlaveId(offer.getSlaveId())
                .addAllResources(resources);

        if (useDocker) {
            LOGGER.info("Using Docker to start Elasticsearch cloud mesos on slaves");
            Protos.ContainerInfo.Builder containerInfo = Protos.ContainerInfo.newBuilder();

            Protos.ContainerInfo.DockerInfo docker = Protos.ContainerInfo.DockerInfo.newBuilder()
                    .setImage("mesos/elasticsearch-cloud-mesos").build();
            containerInfo.setDocker(docker);
            containerInfo.setType(Protos.ContainerInfo.Type.DOCKER);
            taskInfoBuilder.setContainer(containerInfo);

            taskInfoBuilder
                    .setCommand(Protos.CommandInfo.newBuilder()
                            .addArguments("elasticsearch")
//                            .addArguments("--cloud.mesos.master").addArguments("http://" + masterUrl)
                            .addArguments("--logger.discovery").addArguments("DEBUG")
                            .addArguments("--discovery.type").addArguments("mesos")
                            .setShell(false))
                    .build();
        } else {
            LOGGER.info("NOT using Docker to start Elasticsearch cloud mesos on slaves");
            Protos.ExecutorInfo executorInfo = Protos.ExecutorInfo.newBuilder()
                    .setExecutorId(Protos.ExecutorID.newBuilder().setValue("" + UUID.randomUUID()))
                    .setFrameworkId(frameworkId)
                    .setCommand(Protos.CommandInfo.newBuilder()
                            .addUris(Protos.CommandInfo.URI.newBuilder().setValue("hdfs://" + namenode + Binaries.ES_EXECUTOR_HDFS_PATH))
                            .addUris(Protos.CommandInfo.URI.newBuilder().setValue("hdfs://" + namenode + Binaries.ES_CLOUD_MESOS_HDFS_PATH))
                            .setValue("java -jar " + Binaries.ES_EXECUTOR_JAR))
                    .setName("" + UUID.randomUUID())
                    .addAllResources(resources)
                    .build();

            taskInfoBuilder.setExecutor(executorInfo);
        }

        return taskInfoBuilder.build();
    }

    @Override
    public void offerRescinded(SchedulerDriver driver, Protos.OfferID offerId) {
        LOGGER.info("Offer " + offerId.getValue() + " rescinded");
    }

    @Override
    public void statusUpdate(SchedulerDriver driver, Protos.TaskStatus status) {
        LOGGER.info("Status update - Task ID: " + status.getTaskId() + ", State: " + status.getState());
    }

    @Override
    public void frameworkMessage(SchedulerDriver driver, Protos.ExecutorID executorId, Protos.SlaveID slaveId, byte[] data) {
        LOGGER.info("Framework Message - Executor: " + executorId.getValue() + ", SlaveID: " + slaveId.getValue());
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
        LOGGER.info("Executor lost: " + executorId.getValue() +
                "on slave " + slaveId.getValue() +
                "with status " + status);
    }

    @Override
    public void error(SchedulerDriver driver, String message) {
        LOGGER.error("Error: " + message);
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

        //TODO: return tasks.stream().map(Task::getHostname).noneMatch(Predicate.isEqual(offer.getHostname()));
    }

    private boolean haveEnoughNodes() {
        return tasks.size() == numberOfHwNodes;
    }

    public Set<Task> getTasks() {
        return tasks;
    }
}
