package org.apache.mesos.elasticsearch.scheduler;

import com.google.protobuf.ByteString;
import org.apache.log4j.Logger;
import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.common.Discovery;
import org.apache.mesos.elasticsearch.common.cli.ElasticsearchCLIParameter;
import org.apache.mesos.elasticsearch.common.cli.HostsCLIParameter;
import org.apache.mesos.elasticsearch.scheduler.configuration.ExecutorEnvironmentalVariables;
import org.apache.mesos.elasticsearch.scheduler.state.ClusterState;
import org.apache.mesos.elasticsearch.scheduler.state.FrameworkState;
import org.apache.mesos.elasticsearch.scheduler.util.Clock;
import org.apache.mesos.elasticsearch.scheduler.util.NetworkUtils;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;

/**
 * Factory for creating {@link Protos.TaskInfo}s
 */
public class TaskInfoFactory {

    private static final Logger LOGGER = Logger.getLogger(TaskInfoFactory.class);

    public static final String TASK_DATE_FORMAT = "yyyyMMdd'T'HHmmss.SSS'Z'";

    public static final String SETTINGS_PATH_VOLUME = "/tmp/config";

    public static final String SETTINGS_DATA_VOLUME_CONTAINER = "/data";

    private FrameworkState frameworkState;
    private final ClusterState clusterState;
    private final NetworkUtils networkUtils = new NetworkUtils();

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
        this.frameworkState = frameworkState;
        List<Integer> ports;
        if (configuration.getElasticsearchPorts().isEmpty()) {
            ports = Resources.selectTwoPortsFromRange(offer.getResourcesList());
        } else {
            ports = configuration.getElasticsearchPorts();
        }

        List<Protos.Resource> acceptedResources = Resources.buildFrameworkResources(configuration);

        LOGGER.info("Creating Elasticsearch task [client port: " + ports.get(0) + ", transport port: " + ports.get(1) + "]");

        Protos.DiscoveryInfo.Builder discovery = Protos.DiscoveryInfo.newBuilder();
        Protos.Ports.Builder discoveryPorts = Protos.Ports.newBuilder();
        acceptedResources.add(Resources.singlePortRange(ports.get(0), configuration.getFrameworkRole()));
        acceptedResources.add(Resources.singlePortRange(ports.get(1), configuration.getFrameworkRole()));
        discoveryPorts.addPorts(Discovery.CLIENT_PORT_INDEX, Protos.Port.newBuilder().setNumber(ports.get(0)).setName(Discovery.CLIENT_PORT_NAME));
        discoveryPorts.addPorts(Discovery.TRANSPORT_PORT_INDEX, Protos.Port.newBuilder().setNumber(ports.get(1)).setName(Discovery.TRANSPORT_PORT_NAME));
        discovery.setPorts(discoveryPorts);
        discovery.setVisibility(Protos.DiscoveryInfo.Visibility.EXTERNAL);

        String hostname = offer.getHostname();
        LOGGER.debug("Attempting to resolve hostname: " + hostname);
        InetSocketAddress address = new InetSocketAddress(hostname, ports.get(0));
        String hostAddress = address.getAddress().getHostAddress(); // Note this will always resolve because of the check in OfferStrategy

        return Protos.TaskInfo.newBuilder()
                .setName(configuration.getTaskName())
                .setData(toData(offer.getHostname(), hostAddress, clock.nowUTC()))
                .setTaskId(Protos.TaskID.newBuilder().setValue(taskId(offer, clock)))
                .setSlaveId(offer.getSlaveId())
                .addAllResources(acceptedResources)
                .setDiscovery(discovery)
                .setExecutor(newExecutorInfo(configuration, discoveryPorts.build())).build();
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

    private Protos.ExecutorInfo.Builder newExecutorInfo(Configuration configuration, Protos.Ports discoveryPorts) {
        Protos.ExecutorInfo.Builder executorInfoBuilder = Protos.ExecutorInfo.newBuilder()
                .setExecutorId(Protos.ExecutorID.newBuilder().setValue(UUID.randomUUID().toString()))
                .setFrameworkId(frameworkState.getFrameworkID())
                .setName("elasticsearch-executor-" + UUID.randomUUID().toString())
                .setCommand(newCommandInfo(configuration));
        if (configuration.isFrameworkUseDocker()) {

            List<Protos.ContainerInfo.DockerInfo.PortMapping> portMappings = discoveryPorts.getPortsList().stream().map(
                    port -> Protos.ContainerInfo.DockerInfo.PortMapping.newBuilder().setHostPort(port.getNumber()).setContainerPort(port.getNumber()).build()
            ).collect(Collectors.toList());
            Protos.ContainerInfo.DockerInfo.Builder containerBuilder = Protos.ContainerInfo.DockerInfo.newBuilder()
                    .setImage(configuration.getExecutorImage())
                    .setForcePullImage(configuration.getExecutorForcePullImage())
                    .setNetwork(Protos.ContainerInfo.DockerInfo.Network.BRIDGE)
                    .addAllPortMappings(portMappings);

            executorInfoBuilder.setContainer(Protos.ContainerInfo.newBuilder()
                    .setType(Protos.ContainerInfo.Type.DOCKER)
                    .setDocker(containerBuilder)
                    .addVolumes(Protos.Volume.newBuilder().setHostPath(SETTINGS_PATH_VOLUME).setContainerPath(SETTINGS_PATH_VOLUME).setMode(Protos.Volume.Mode.RO)) // Temporary fix until we get a data container.
                    .addVolumes(Protos.Volume.newBuilder().setContainerPath(SETTINGS_DATA_VOLUME_CONTAINER).setHostPath(configuration.getDataDir()).setMode(Protos.Volume.Mode.RW).build())
                    .build())
                    .addResources(Resources.cpus(configuration.getExecutorCpus(), configuration.getFrameworkRole()))
                    .addResources(Resources.mem(configuration.getExecutorMem(), configuration.getFrameworkRole()))
            ;
        }
        return executorInfoBuilder;
    }

    private Protos.CommandInfo.Builder newCommandInfo(Configuration configuration) {
        ExecutorEnvironmentalVariables executorEnvironmentalVariables = new ExecutorEnvironmentalVariables(configuration);
        List<String> args = new ArrayList<>();
        addIfNotEmpty(args, ElasticsearchCLIParameter.ELASTICSEARCH_SETTINGS_LOCATION, configuration.getElasticsearchSettingsLocation());
        addIfNotEmpty(args, ElasticsearchCLIParameter.ELASTICSEARCH_CLUSTER_NAME, configuration.getElasticsearchClusterName());
        args.addAll(asList(ElasticsearchCLIParameter.ELASTICSEARCH_NODES, Integer.toString(configuration.getElasticsearchNodes())));
        List<Protos.TaskInfo> taskList = clusterState.getTaskList();
        String hostAddress = "";
        if (taskList.size() > 0) {
            Protos.TaskInfo taskInfo = taskList.get(0);
            String taskId = taskInfo.getTaskId().getValue();
            InetSocketAddress transportAddress = clusterState.getGuiTaskList().get(taskId).getTransportAddress();
            hostAddress = networkUtils.addressToString(transportAddress, configuration.getIsUseIpAddress()).replace("http://", "");
        }
        addIfNotEmpty(args, HostsCLIParameter.ELASTICSEARCH_HOST, hostAddress);

        Protos.CommandInfo.Builder commandInfoBuilder = Protos.CommandInfo.newBuilder()
                .setEnvironment(Protos.Environment.newBuilder().addAllVariables(executorEnvironmentalVariables.getList()));

        if (configuration.isFrameworkUseDocker()) {
            commandInfoBuilder
                    .setShell(false)
                    .addAllArguments(args)
                    .setContainer(Protos.CommandInfo.ContainerInfo.newBuilder().setImage(configuration.getExecutorImage()).build());
        } else {
            String address = configuration.getFrameworkFileServerAddress();
            if (address == null) {
                throw new NullPointerException("Webserver address is null");
            }
            String httpPath =  address + "/get/" + SimpleFileServer.ES_EXECUTOR_JAR;
            LOGGER.debug("Using file server: " + httpPath);
            commandInfoBuilder
                    .setValue(configuration.getJavaHome() + "java $JAVA_OPTS -jar ./" + SimpleFileServer.ES_EXECUTOR_JAR)
                    .addAllArguments(args)
                    .addUris(Protos.CommandInfo.URI.newBuilder().setValue(httpPath));
        }

        return commandInfoBuilder;
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
