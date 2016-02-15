package org.apache.mesos.elasticsearch.scheduler;

import com.google.protobuf.ByteString;
import org.apache.log4j.Logger;
import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.common.Discovery;
import org.apache.mesos.elasticsearch.common.util.NetworkUtils;
import org.apache.mesos.elasticsearch.scheduler.state.ClusterState;
import org.apache.mesos.elasticsearch.scheduler.state.FrameworkState;
import org.apache.mesos.elasticsearch.scheduler.util.Clock;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;

/**
 * Factory for creating {@link Protos.TaskInfo}s
 */
public class TaskInfoFactory {

    private static final Logger LOGGER = Logger.getLogger(TaskInfoFactory.class);

    public static final String TASK_DATE_FORMAT = "yyyyMMdd'T'HHmmss.SSS'Z'";
    public static final String CONTAINER_PATH_DATA = "/usr/share/elasticsearch/data";
    public static final String CONTAINER_PATH_CONF = "/usr/share/elasticsearch/config";
    public static final String HOST_PATH_HOME = "./es_home";
    public static final String HOST_PATH_CONF = "./.";

    private final ClusterState clusterState;

    public TaskInfoFactory(ClusterState clusterState) {
        this.clusterState = clusterState;
    }

    /**
     * Creates TaskInfo for Elasticsearch execcutor running in a Docker container
     *
     * @param configuration configuation of the framework
     * @param offer with resources to run the executor with
     *
     * @return TaskInfo
     */
    public Protos.TaskInfo createTask(Configuration configuration, FrameworkState frameworkState, Protos.Offer offer, Clock clock) {
        if (configuration.isFrameworkUseDocker()) {
            LOGGER.debug("Building Docker task");
            Protos.TaskInfo taskInfo = buildDockerTask(offer, configuration, clock);
            LOGGER.debug(taskInfo.toString());
            return taskInfo;
        } else {
            LOGGER.debug("Building native task");
            Protos.TaskInfo taskInfo = buildNativeTask(offer, configuration, clock);
            LOGGER.debug(taskInfo.toString());
            return taskInfo;
        }
    }

    private Protos.TaskInfo buildNativeTask(Protos.Offer offer, Configuration configuration, Clock clock) {
        final List<Integer> ports = getPorts(offer, configuration);
        final List<Protos.Resource> resources = getResources(configuration, ports);
        final Protos.DiscoveryInfo discovery = getDiscovery(ports);

        final String hostAddress = resolveHostAddress(offer, ports);

        LOGGER.info("Creating Elasticsearch task with resources: " + resources.toString());

        final List<String> args = getArguments(configuration, discovery);

        return Protos.TaskInfo.newBuilder()
                .setName(configuration.getTaskName())
                .setData(toData(offer.getHostname(), hostAddress, clock.nowUTC()))
                .setTaskId(Protos.TaskID.newBuilder().setValue(taskId(offer, clock)))
                .setSlaveId(offer.getSlaveId())
                .addAllResources(resources)
                .setDiscovery(discovery)
                .setCommand(nativeCommand(configuration, args))
                .build();
    }

    private Protos.TaskInfo buildDockerTask(Protos.Offer offer, Configuration configuration, Clock clock) {
        final List<Integer> ports = getPorts(offer, configuration);
        final List<Protos.Resource> resources = getResources(configuration, ports);
        final Protos.DiscoveryInfo discovery = getDiscovery(ports);

        final String hostAddress = resolveHostAddress(offer, ports);

        LOGGER.info("Creating Elasticsearch task with resources: " + resources.toString());

        final Protos.TaskID taskId = Protos.TaskID.newBuilder().setValue(taskId(offer, clock)).build();
        final List<String> args = getArguments(configuration, discovery);
        final Protos.ContainerInfo containerInfo = getContainer(configuration, taskId);

        return Protos.TaskInfo.newBuilder()
                .setName(configuration.getTaskName())
                .setData(toData(offer.getHostname(), hostAddress, clock.nowUTC()))
                .setTaskId(taskId)
                .setSlaveId(offer.getSlaveId())
                .addAllResources(resources)
                .setDiscovery(discovery)
                .setCommand(dockerCommand(configuration, args))
                .setContainer(containerInfo)
                .build();
    }

    private String resolveHostAddress(Protos.Offer offer, List<Integer> ports) {
        String hostname = offer.getHostname();
        LOGGER.debug("Attempting to resolve hostname: " + hostname);
        InetSocketAddress address = new InetSocketAddress(hostname, ports.get(0));
        return address.getAddress().getHostAddress(); // Note this will always resolve because of the check in OfferStrategy
    }

    private List<Integer> getPorts(Protos.Offer offer, Configuration configuration) {
        List<Integer> ports;
        if (configuration.getElasticsearchPorts().isEmpty()) {
            ports = Resources.selectTwoPortsFromRange(offer.getResourcesList());
        } else {
            ports = configuration.getElasticsearchPorts();
        }
        return ports;
    }

    private List<Protos.Resource> getResources(Configuration configuration, List<Integer> ports) {
        List<Protos.Resource> acceptedResources = Resources.buildFrameworkResources(configuration);
        acceptedResources.add(Resources.singlePortRange(ports.get(0), configuration.getFrameworkRole()));
        acceptedResources.add(Resources.singlePortRange(ports.get(1), configuration.getFrameworkRole()));
        return acceptedResources;
    }

    private Protos.DiscoveryInfo getDiscovery(List<Integer> ports) {
        Protos.DiscoveryInfo.Builder discovery = Protos.DiscoveryInfo.newBuilder();
        Protos.Ports.Builder discoveryPorts = Protos.Ports.newBuilder();
        discoveryPorts.addPorts(Discovery.CLIENT_PORT_INDEX, Protos.Port.newBuilder().setNumber(ports.get(0)).setName(Discovery.CLIENT_PORT_NAME));
        discoveryPorts.addPorts(Discovery.TRANSPORT_PORT_INDEX, Protos.Port.newBuilder().setNumber(ports.get(1)).setName(Discovery.TRANSPORT_PORT_NAME));
        discovery.setPorts(discoveryPorts);
        discovery.setVisibility(Protos.DiscoveryInfo.Visibility.EXTERNAL);
        return discovery.build();
    }

    private Protos.ContainerInfo getContainer(Configuration configuration, Protos.TaskID taskID) {
        final Protos.ContainerInfo.Builder builder = Protos.ContainerInfo.newBuilder()
                .setType(Protos.ContainerInfo.Type.DOCKER)
                .setDocker(Protos.ContainerInfo.DockerInfo.newBuilder()
                        .addParameters(Protos.Parameter.newBuilder().setKey("env").setValue("MESOS_TASK_ID=" + taskID.getValue()))
                        .setImage(configuration.getExecutorImage())
                        .setForcePullImage(configuration.getExecutorForcePullImage())
                        .setNetwork(Protos.ContainerInfo.DockerInfo.Network.HOST))
                .addVolumes(Protos.Volume.newBuilder()
                        .setHostPath(configuration.getDataDir())
                        .setContainerPath(CONTAINER_PATH_DATA)
                        .setMode(Protos.Volume.Mode.RW)
                        .build());
        if (!configuration.getElasticsearchSettingsLocation().isEmpty()) {
            final Path path = Paths.get(configuration.getElasticsearchSettingsLocation());
            final Path fileName = path.getFileName();
            if (fileName == null) {
                throw new IllegalArgumentException("Cannot parse filename from settings location. Please include the /elasticsearch.yml in the settings location.");
            }
            final String settingsFilename = fileName.toString();
            builder.addVolumes(Protos.Volume.newBuilder()
                    .setHostPath("./" + settingsFilename) // Because the file has been uploaded by the uris.
                    .setContainerPath(CONTAINER_PATH_CONF)
                    .setMode(Protos.Volume.Mode.RO)
                    .build());
        }
        return builder
                .build();
    }

    private Protos.CommandInfo dockerCommand(Configuration configuration, List<String> args) {
        final Protos.CommandInfo.Builder builder = Protos.CommandInfo.newBuilder()
                .setShell(false)
                .addAllArguments(args);
        if (!configuration.getElasticsearchSettingsLocation().isEmpty()) {
            builder.addUris(Protos.CommandInfo.URI.newBuilder().setValue(configuration.getElasticsearchSettingsLocation()));
        }
        return builder
                .build();
    }

    private Protos.CommandInfo nativeCommand(Configuration configuration, List<String> args) {
        String address = configuration.getFrameworkFileServerAddress();
        if (address == null) {
            throw new NullPointerException("Webserver address is null");
        }
        String httpPath = address + "/get/" + Configuration.ES_TAR;
        String folders = configuration.getDataDir();
        String mkdir = "sudo mkdir -p " + folders + "; ";
        String chown = "sudo chown -R nobody:nogroup " + folders + "; ";
        String command = mkdir +
                        chown +
                        " sudo su -s /bin/bash -c \""
                        + Configuration.ES_BINARY
                        + " "
                        + args.stream().collect(Collectors.joining(" "))
                        + "\" nobody";
        final Protos.CommandInfo.Builder builder = Protos.CommandInfo.newBuilder()
                .setValue(command)
                .setUser("nobody")
                .addUris(Protos.CommandInfo.URI.newBuilder().setValue(httpPath));
        if (!configuration.getElasticsearchSettingsLocation().isEmpty()) {
            builder.addUris(Protos.CommandInfo.URI.newBuilder().setValue(configuration.getElasticsearchSettingsLocation()));
        }
        return builder
                .build();
    }


    private List<String> getArguments(Configuration configuration, Protos.DiscoveryInfo discoveryInfo) {
        List<String> args = new ArrayList<>();
        List<Protos.TaskInfo> taskList = clusterState.getTaskList();
        String hostAddress = "";
        if (taskList.size() > 0) {
            Protos.TaskInfo taskInfo = taskList.get(0);
            String taskId = taskInfo.getTaskId().getValue();
            InetSocketAddress transportAddress = clusterState.getGuiTaskList().get(taskId).getTransportAddress();
            hostAddress = NetworkUtils.addressToString(transportAddress, configuration.getIsUseIpAddress()).replace("http://", "");
        }
        addIfNotEmpty(args, "--default.discovery.zen.ping.unicast.hosts", hostAddress);
        args.add("--default.http.port=" + discoveryInfo.getPorts().getPorts(Discovery.CLIENT_PORT_INDEX).getNumber());
        args.add("--default.transport.tcp.port=" + discoveryInfo.getPorts().getPorts(Discovery.TRANSPORT_PORT_INDEX).getNumber());
        args.add("--default.cluster.name=" + configuration.getElasticsearchClusterName());
        args.add("--default.node.master=true");
        args.add("--default.node.data=true");
        args.add("--default.node.local=false");
        args.add("--default.index.number_of_replicas=0");
        args.add("--default.index.auto_expand_replicas=0-all");
        if (!configuration.isFrameworkUseDocker()) {
            args.add("--path.home=" + HOST_PATH_HOME); // Cannot be overidden
            args.add("--default.path.data=" + configuration.getDataDir());
            args.add("--path.conf=" + HOST_PATH_CONF); // Cannot be overidden
        } else {
            args.add("--path.data=" + CONTAINER_PATH_DATA); // Cannot be overidden
        }
        args.add("--default.bootstrap.mlockall=true");
        args.add("--default.network.bind_host=0.0.0.0");
        args.add("--default.network.publish_host=_non_loopback:ipv4_");
        args.add("--default.gateway.recover_after_nodes=1");
        args.add("--default.gateway.expected_nodes=1");
        args.add("--default.indices.recovery.max_bytes_per_sec=100mb");
        args.add("--default.discovery.type=zen");
        args.add("--default.discovery.zen.fd.ping_timeout=30s");
        args.add("--default.discovery.zen.fd.ping_interval=1s");
        args.add("--default.discovery.zen.fd.ping_retries=30");
        args.add("--default.discovery.zen.ping.multicast.enabled=false");


        return args;
    }

    public ByteString toData(String hostname, String ipAddress, ZonedDateTime zonedDateTime) {
        Properties data = new Properties();
        data.put("hostname", hostname);
        data.put("ipAddress", ipAddress);
        data.put("startedAt", zonedDateTime.toString());

        StringWriter writer = new StringWriter();
        try {
            data.store(new PrintWriter(writer), "Task metadata");
        } catch (IOException e) {
            throw new RuntimeException("Failed to write task metadata", e);
        }
        return ByteString.copyFromUtf8(writer.getBuffer().toString());
    }

    private void addIfNotEmpty(List<String> args, String key, String value) {
        if (!value.isEmpty()) {
            args.addAll(asList(key, value));
        }
    }

    private String taskId(Protos.Offer offer, Clock clock) {
        String date = new SimpleDateFormat(TASK_DATE_FORMAT).format(clock.now());
        return String.format("elasticsearch_%s_%s", offer.getHostname(), date);
    }

    public static Task parse(Protos.TaskInfo taskInfo, Protos.TaskStatus taskStatus, Clock clock) {
        Properties data = new Properties();
        try {
            data.load(taskInfo.getData().newInput());
        } catch (IOException e) {
            throw new RuntimeException("Failed to parse properties", e);
        }

        String hostName = Optional.ofNullable(data.getProperty("hostname")).orElseGet(() -> {
            LOGGER.error("Hostname is empty. Reported IP addresses will be incorrect.");
            return "";
        });
        String ipAddress = data.getProperty("ipAddress", hostName);

        final ZonedDateTime startedAt = Optional.ofNullable(data.getProperty("startedAt"))
                .map(s -> s.endsWith("...") ? s.substring(0, 29) : s) //We're convert dates that was capped with Properties.list() method, see https://github.com/mesos/elasticsearch/pull/367
                .map(ZonedDateTime::parse)
                .orElseGet(clock::nowUTC)
                .withZoneSameInstant(ZoneOffset.UTC);

        if (!taskInfo.getDiscovery().isInitialized()) {
            throw new IndexOutOfBoundsException("TaskInfo has no discovery information.");
        }

        return new Task(
                hostName,
                taskInfo.getTaskId().getValue(),
                taskStatus == null ? Protos.TaskState.TASK_STAGING : taskStatus.getState(),
                startedAt,
                new InetSocketAddress(ipAddress, taskInfo.getDiscovery().getPorts().getPorts(Discovery.CLIENT_PORT_INDEX).getNumber()),
                new InetSocketAddress(ipAddress, taskInfo.getDiscovery().getPorts().getPorts(Discovery.TRANSPORT_PORT_INDEX).getNumber())
        );
    }
}
