package org.apache.mesos.elasticsearch.scheduler;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.cli.*;
import org.apache.log4j.Logger;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.ContainerInfo.DockerInfo.PortMapping;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.elasticsearch.common.Binaries;
import org.apache.mesos.elasticsearch.common.Configuration;
import org.apache.mesos.elasticsearch.common.Discovery;
import org.apache.mesos.elasticsearch.common.HostResolver;
import org.apache.mesos.elasticsearch.common.Resources;

import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

/**
 * Scheduler for Elasticsearch.
 */
@SuppressWarnings({"PMD.TooManyMethods", "PMD.CyclomaticComplexity"})
public class ElasticsearchScheduler implements Scheduler, Runnable {

    public static final Logger LOGGER = Logger.getLogger(ElasticsearchScheduler.class.toString());

    public static final String TASK_DATE_FORMAT = "yyyyMMdd'T'HHmmss.SSS'Z'";

    // DCOS Certification requirement 01
    // The time before Mesos kills a scheduler and tasks if it has not recovered.
    // Mesos will kill framework after 1 month if marathon does not restart.
    private static final double FAILOVER_TIMEOUT = 2592000;
    public static final int ZOOKEEPER_PORT = 2181;

    private final State state;

    Clock clock = new Clock();

    Set<Task> tasks = new HashSet<>();

    private CountDownLatch initialized = new CountDownLatch(1);

    private int numberOfHwNodes;

    private String master;

    private String dnsHost;

    private boolean useDocker;

    private Protos.FrameworkID frameworkId;

    private SimpleFileServer fileServer;

    public ElasticsearchScheduler(String master, String dnsHost, int numberOfHwNodes, boolean useDocker, State state, SimpleFileServer fileServer) {
        this.master = master;
        this.dnsHost = dnsHost;
        this.numberOfHwNodes = numberOfHwNodes;
        this.useDocker = useDocker;
        this.state = state;
        this.fileServer = fileServer;
    }

    public static void main(String[] args) {
        Options options = new Options();
        options.addOption("m", "master host or IP", true, "master host or IP");
        options.addOption("dns", "DNS host", true, "DNS host");
        options.addOption("n", "numHardwareNodes", true, "number of hardware nodes");
        options.addOption("d", "useDocker", false, "use docker to launch Elasticsearch");
        options.addOption("zk", "ZookeeperNode", true, "Zookeeper IP address and port");
        CommandLineParser parser = new BasicParser();
        try {
            CommandLine cmd = parser.parse(options, args);
            String masterHost = cmd.getOptionValue("m");
            String dnsHost = cmd.getOptionValue("dns");
            String numberOfHwNodesString = cmd.getOptionValue("n");
            String zkNode = cmd.getOptionValue("zk");
            if (masterHost == null || numberOfHwNodesString == null || zkNode == null) {
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

            InetAddress zkAddress = HostResolver.resolve(zkNode);
            if (zkAddress == null) {
                LOGGER.error("Could not resolve ZK node : " + zkNode);
                System.exit(-1);
            }

            LOGGER.info("Starting ElasticSearch on Mesos - [master: " + masterHost + ", numHwNodes: " + numberOfHwNodes + ", docker: " + (useDocker ? "enabled" : "disabled") + ", dns: " + dnsHost + "]");
            ZooKeeperStateInterface zkState = new ZooKeeperStateInterfaceImpl(zkAddress.getHostAddress() + ":" + ZOOKEEPER_PORT);
            State state = new State(zkState);

            SimpleFileServer fileServer = new SimpleFileServer(Configuration.FILE_SERVER_PORT);

            final ElasticsearchScheduler scheduler = new ElasticsearchScheduler(masterHost, dnsHost, numberOfHwNodes, useDocker, state, fileServer);

            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                @Override
                public void run() {
                    scheduler.onShutdown();
                }
            }));

            Thread schedThred = new Thread(scheduler);
            schedThred.start();
            scheduler.waitUntilInit();
        } catch (ParseException e) {
            printUsage(options);
        }
    }

    private void onShutdown() {
        LOGGER.info("On shutdown...");

        LOGGER.info("Shutting down Elasticsearch file server...");
        fileServer.stop();
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
        LOGGER.info("Starting up ...");
        final Protos.FrameworkInfo.Builder frameworkBuilder = Protos.FrameworkInfo.newBuilder();
        frameworkBuilder.setUser("");
        frameworkBuilder.setName(Configuration.FRAMEWORK_NAME);
        frameworkBuilder.setCheckpoint(true);
        frameworkBuilder.setFailoverTimeout(FAILOVER_TIMEOUT);
        frameworkBuilder.setCheckpoint(true); // DCOS certification 04 - Checkpointing is enabled.

        try {
            Protos.FrameworkID frameworkID = this.getState().getFrameworkID(); // DCOS certification 02
            if (frameworkID != null) {
                LOGGER.info("Found previous frameworkID: " + frameworkID);
                frameworkBuilder.setId(frameworkID);
            }
        } catch (InterruptedException | ExecutionException
                | InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }

        fileServer.start();

        final MesosSchedulerDriver driver = new MesosSchedulerDriver(this, frameworkBuilder.build(), this.master + ":" + Configuration.MESOS_PORT);
        driver.run();
    }

    @Override
    public void registered(SchedulerDriver driver, Protos.FrameworkID frameworkId, Protos.MasterInfo masterInfo) {
        this.frameworkId = frameworkId;
        try {
            getState().setFrameworkId(frameworkId); // DCOS certification 02
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
        LOGGER.info("Framework registered as " + frameworkId.getValue());

        List<Protos.Resource> resources = buildResources();

        Protos.Request request = Protos.Request.newBuilder()
                .addAllResources(resources)
                .build();

        List<Protos.Request> requests = Collections.singletonList(request);
        driver.requestResources(requests);
    }

    private static List<Protos.Resource> buildResources() {
        Protos.Resource cpus = Resources.cpus(Configuration.CPUS);
        Protos.Resource mem = Resources.mem(Configuration.MEM);
        Protos.Resource disk = Resources.disk(Configuration.DISK);
        Protos.Resource ports = Resources.portRange(Configuration.BEGIN_PORT, Configuration.END_PORT);
        return Arrays.asList(cpus, mem, disk, ports);
    }

    @Override
    public void reregistered(SchedulerDriver driver, Protos.MasterInfo masterInfo) {
        LOGGER.info("Framework re-registered");
    }

    @Override
    public void resourceOffers(SchedulerDriver driver, List<Protos.Offer> offers) {
        for (Protos.Offer offer : offers) {
            if (!isOfferGood(offer)) {
                driver.declineOffer(offer.getId()); // DCOS certification 05
                LOGGER.info("Declined offer: Offer is not sufficient");
            } else if (haveEnoughNodes()) {
                driver.declineOffer(offer.getId()); // DCOS certification 05
                LOGGER.info("Declined offer: Node " + offer.getHostname() + " already has an Elasticsearch task");
            } else {
                LOGGER.info("Accepted offer: " + offer.getHostname());

                String id = taskId(offer);

                Protos.TaskInfo taskInfo = buildTask(driver, offer.getResourcesList(), offer, id);
                LOGGER.info("TaskInfo: " + taskInfo.toString());

                driver.launchTasks(Collections.singleton(offer.getId()), Collections.singleton(taskInfo));
                tasks.add(new Task(offer.getHostname(), id));
            }
        }

        if (haveEnoughNodes()) {
            initialized.countDown();
        }
    }

    @SuppressWarnings("PMD.ExcessiveMethodLength")
    private Protos.TaskInfo buildTask(SchedulerDriver driver, List<Protos.Resource> offeredResources, Protos.Offer offer, String id) {
        List<Protos.Resource> acceptedResources = new ArrayList<>();

        addAllScalarResources(offeredResources, acceptedResources);

        List<Integer> ports = selectPorts(offeredResources);
        Protos.DiscoveryInfo.Builder discovery = Protos.DiscoveryInfo.newBuilder();
        Protos.Ports.Builder discoveryPorts = Protos.Ports.newBuilder();
        if (ports.size() != 2) {
            LOGGER.info("Declined offer: Offer did not contain 2 ports for Elasticsearch client and transport connection");
            driver.declineOffer(offer.getId());
        } else {
            LOGGER.info("Elasticsearch client port " + ports.get(0));
            LOGGER.info("Elasticsearch transport port " + ports.get(1));
            acceptedResources.add(Resources.singlePortRange(ports.get(0)));
            acceptedResources.add(Resources.singlePortRange(ports.get(1)));
            discoveryPorts.addPorts(Discovery.CLIENT_PORT_INDEX, Protos.Port.newBuilder().setNumber(ports.get(0)).setName(Discovery.CLIENT_PORT_NAME));
            discoveryPorts.addPorts(Discovery.TRANSPORT_PORT_INDEX, Protos.Port.newBuilder().setNumber(ports.get(1)).setName(Discovery.TRANSPORT_PORT_NAME));
            discovery.setPorts(discoveryPorts);
            discovery.setVisibility(Protos.DiscoveryInfo.Visibility.EXTERNAL);
        }

        Protos.TaskInfo.Builder taskInfoBuilder = Protos.TaskInfo.newBuilder()
                .setName(Configuration.TASK_NAME)
                .setTaskId(Protos.TaskID.newBuilder().setValue(id))
                .setSlaveId(offer.getSlaveId())
                .addAllResources(acceptedResources)
                .setDiscovery(discovery);

        if (useDocker) {
            LOGGER.info("Using Docker to start Elasticsearch cloud mesos on slaves");
            Protos.ContainerInfo.Builder containerInfo = Protos.ContainerInfo.newBuilder();
            PortMapping clientPortMapping = PortMapping.newBuilder().setContainerPort(Configuration.ELASTICSEARCH_CLIENT_PORT).setHostPort(ports.get(0)).build();
            PortMapping transportPortMapping = PortMapping.newBuilder().setContainerPort(Configuration.ELASTICSEARCH_TRANSPORT_PORT).setHostPort(ports.get(1)).build();

            InetAddress masterAddress = HostResolver.resolve(master);
            if (masterAddress == null) {
                LOGGER.error("Could not resolve master host : " + master);
                return taskInfoBuilder.build();
            }

            InetAddress dnsAddress = HostResolver.resolve(dnsHost);
            if (dnsAddress == null) {
                LOGGER.error("Could not resolve DNS host: " + dnsHost);
                return taskInfoBuilder.build();
            }

            InetAddress slaveAddress = HostResolver.resolve(offer.getHostname());
            if (slaveAddress == null) {
                LOGGER.error("Could not resolve slave host: " + offer.getHostname());
                return taskInfoBuilder.build();
            }

            Protos.ContainerInfo.DockerInfo.Builder docker = Protos.ContainerInfo.DockerInfo.newBuilder()
                    .setNetwork(Protos.ContainerInfo.DockerInfo.Network.BRIDGE)
                    .setImage("mesos/elasticsearch-cloud-mesos")
                    .addPortMappings(clientPortMapping)
                    .addPortMappings(transportPortMapping);

            if (dnsHost != null) {
                docker.addParameters(Protos.Parameter.newBuilder().setKey("dns").setValue(dnsAddress.getHostAddress()));
            }

            containerInfo.setDocker(docker.build());
            containerInfo.setType(Protos.ContainerInfo.Type.DOCKER);
            taskInfoBuilder.setContainer(containerInfo);
            taskInfoBuilder
                    .setCommand(Protos.CommandInfo.newBuilder()
                            .addArguments("elasticsearch")
                            .addArguments("--network.publish_host").addArguments(slaveAddress.getHostAddress())
                            .addArguments("--transport.publish_port").addArguments(String.valueOf(ports.get(1)))
                            .addArguments("--node.master").addArguments("true")
                            .addArguments("--cloud.mesos.master").addArguments("http://" + masterAddress.getHostAddress() + ":" + Configuration.MESOS_PORT)
                            .addArguments("--logger.discovery").addArguments("DEBUG")
                            .addArguments("--logger.cloud.mesos").addArguments("DEBUG")
                            .addArguments("--discovery.type").addArguments("mesos")
                            .setShell(false))
                    .build();
        } else {
            LOGGER.info("Using Executor to start Elasticsearch cloud mesos on slaves");

            Protos.CommandInfo.Builder commandInfo = Protos.CommandInfo.newBuilder()
                    .setValue("java -jar " + Binaries.ES_EXECUTOR_JAR)
                    .addUris(Protos.CommandInfo.URI.newBuilder().setValue(fileServer.getExecutorJarUrl()))
                    .addUris(Protos.CommandInfo.URI.newBuilder().setValue(fileServer.getCloudMesosUrl()));

            String master = HostResolver.resolve(this.master).getHostAddress();

            LOGGER.info("Master: " + master);

            Protos.ExecutorInfo.Builder executorInfo = Protos.ExecutorInfo.newBuilder()
                    .setCommand(commandInfo)
                    .setExecutorId(Protos.ExecutorID.newBuilder().setValue(UUID.randomUUID().toString()))
                    .addAllResources(acceptedResources)
                    .setData(ByteString.copyFromUtf8(master))
                    .setFrameworkId(frameworkId)
                    .setName("" + UUID.randomUUID());

            taskInfoBuilder.setExecutor(executorInfo);
        }

        return taskInfoBuilder.build();
    }

    private List<Integer> selectPorts(List<Protos.Resource> offeredResources) {
        List<Integer> ports = new ArrayList<>();
        for (Protos.Resource resource : offeredResources) {
            if (resource.getType().equals(Protos.Value.Type.RANGES)) {
                for (Protos.Value.Range range : resource.getRanges().getRangeList()) {
                    if (ports.size() < 2) {
                        ports.add((int) range.getBegin());
                        if (ports.size() < 2 && range.getBegin() != range.getEnd()) {
                            ports.add((int) range.getBegin() + 1);
                        }
                    }
                }
            }
        }
        return ports;
    }

    private void addAllScalarResources(List<Protos.Resource> offeredResources, List<Protos.Resource> acceptedResources) {
        for (Protos.Resource resource : offeredResources) {
            if (resource.getType().equals(Protos.Value.Type.SCALAR)) {
                Protos.Value.Scalar scalar = Protos.Value.Scalar.newBuilder().setValue(0.1 * resource.getScalar().getValue()).build();
                acceptedResources.add(Protos.Resource.newBuilder().setType(Protos.Value.Type.SCALAR).setName(resource.getName()).setScalar(scalar).build());
            }
        }
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

    public State getState() {
        return state;
    }
}
