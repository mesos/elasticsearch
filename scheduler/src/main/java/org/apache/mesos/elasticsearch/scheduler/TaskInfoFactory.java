package org.apache.mesos.elasticsearch.scheduler;

import com.google.protobuf.ByteString;
import org.apache.log4j.Logger;
import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.common.Discovery;
import org.apache.mesos.elasticsearch.scheduler.state.ClusterState;
import org.apache.mesos.elasticsearch.scheduler.state.FrameworkState;
import org.apache.mesos.elasticsearch.scheduler.util.Clock;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static java.util.Arrays.asList;
import static org.apache.mesos.elasticsearch.common.elasticsearch.ElasticsearchSettings.CONTAINER_DATA_VOLUME;
import static org.apache.mesos.elasticsearch.common.elasticsearch.ElasticsearchSettings.CONTAINER_PATH_SETTINGS;

/**
 * Factory for creating {@link Protos.TaskInfo}s
 */
public class TaskInfoFactory {

    private static final Logger LOGGER = Logger.getLogger(TaskInfoFactory.class);

    public static final String TASK_DATE_FORMAT = "yyyyMMdd'T'HHmmss.SSS'Z'";

    private FrameworkState frameworkState;
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
        this.frameworkState = frameworkState;
        List<Integer> ports;
        if (configuration.getElasticsearchPorts().isEmpty()) {
            ports = Resources.selectTwoPortsFromRange(offer.getResourcesList());
        } else {
            ports = configuration.getElasticsearchPorts();
        }

        List<Protos.Resource> acceptedResources = Resources.buildFrameworkResources(configuration);

        LOGGER.info("Creating Elasticsearch task with resources: " + acceptedResources.toString());

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
                .setCommand(newCommandInfo(configuration))
                .setContainer(newContainerInfo(configuration, discoveryPorts.build())) // TODO (PNW): IF JAR MODE DONT ADD CONTAINER
                .build();
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

    private Protos.ContainerInfo newContainerInfo(Configuration configuration, Protos.Ports discoveryPorts) {
        final Protos.ContainerInfo.DockerInfo.PortMapping httpPort = Protos.ContainerInfo.DockerInfo.PortMapping.newBuilder().setContainerPort(9200).setHostPort(discoveryPorts.getPorts(Discovery.CLIENT_PORT_INDEX).getNumber()).build();
        final Protos.ContainerInfo.DockerInfo.PortMapping transportPort = Protos.ContainerInfo.DockerInfo.PortMapping.newBuilder().setContainerPort(9300).setHostPort(discoveryPorts.getPorts(Discovery.TRANSPORT_PORT_INDEX).getNumber()).build();
        List<Protos.ContainerInfo.DockerInfo.PortMapping> portMappings = Arrays.asList(httpPort, transportPort);
        return Protos.ContainerInfo.newBuilder()
                    .setType(Protos.ContainerInfo.Type.DOCKER)
                .setDocker(Protos.ContainerInfo.DockerInfo.newBuilder()
                        .setImage(configuration.getExecutorImage())
                        .setForcePullImage(configuration.getExecutorForcePullImage())
                        .setNetwork(Protos.ContainerInfo.DockerInfo.Network.BRIDGE)
                        .addAllPortMappings(portMappings))
                    .addVolumes(Protos.Volume.newBuilder().setHostPath(CONTAINER_PATH_SETTINGS).setContainerPath(CONTAINER_PATH_SETTINGS).setMode(Protos.Volume.Mode.RO)) // Temporary fix until we get a data container.
                    .addVolumes(Protos.Volume.newBuilder().setContainerPath(CONTAINER_DATA_VOLUME).setHostPath(configuration.getDataDir()).setMode(Protos.Volume.Mode.RW).build())
                .build();
    }

    private Protos.CommandInfo.Builder newCommandInfo(Configuration configuration) {
        Protos.CommandInfo.Builder commandInfoBuilder = Protos.CommandInfo.newBuilder();
        final List<String> args = Arrays.asList(
                "-Des.network.host=_non_loopback:ipv4_",
                "-Des.discovery.zen.ping.unicast.hosts=172.17.0.1:9300,172.17.0.1:9301,172.17.0.1:9302" // TODO (pnw): Hardcoded IP.
        );
        if (configuration.isFrameworkUseDocker()) {
            commandInfoBuilder
                    .setShell(false)
                    .addAllArguments(args);
        } else {
            LOGGER.info("TODO: Java Binary Stub");
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
