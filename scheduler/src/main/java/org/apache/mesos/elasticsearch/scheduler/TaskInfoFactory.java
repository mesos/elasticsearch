package org.apache.mesos.elasticsearch.scheduler;

import com.google.protobuf.ByteString;
import org.apache.log4j.Logger;
import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.common.Discovery;
import org.apache.mesos.elasticsearch.scheduler.configuration.ExecutorEnvironmentalVariables;
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
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Factory for creating {@link Protos.TaskInfo}s
 */
public class TaskInfoFactory {

    private static final Logger LOGGER = Logger.getLogger(TaskInfoFactory.class);

    public static final String TASK_DATE_FORMAT = "yyyyMMdd'T'HHmmss.SSS'Z'";

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
        //this creates and assigns a unique id to an elastic search node
        long elasticSearchNodeId = configuration.getExternalVolumeDriver() != null && configuration.getExternalVolumeDriver().length() > 0 ?
                clusterState.getElasticNodeId() : ExecutorEnvironmentalVariables.EXTERNAL_VOLUME_NOT_CONFIGURED;

        LOGGER.debug("Elastic Search Node Id: " + elasticSearchNodeId);
        if (configuration.isFrameworkUseDocker()) {
            LOGGER.debug("Building Docker task");
            Protos.TaskInfo taskInfo = buildDockerTask(offer, configuration, clock, elasticSearchNodeId);
            LOGGER.debug(taskInfo.toString());
            return taskInfo;
        } else {
            LOGGER.debug("Building native task");
            Protos.TaskInfo taskInfo = buildNativeTask(offer, configuration, clock, elasticSearchNodeId);
            LOGGER.debug(taskInfo.toString());
            return taskInfo;
        }
    }

    private Protos.TaskInfo buildNativeTask(Protos.Offer offer, Configuration configuration, Clock clock, Long elasticSearchNodeId) {
        final List<Integer> ports = getPorts(offer, configuration);
        final List<Protos.Resource> resources = getResources(configuration, ports);
        final Protos.DiscoveryInfo discovery = getDiscovery(ports, configuration);

        final String hostAddress = resolveHostAddress(offer, ports);

        LOGGER.info("Creating Elasticsearch task with resources: " + resources.toString());

        return Protos.TaskInfo.newBuilder()
                .setName(configuration.getTaskName())
                .setData(toData(offer.getHostname(), hostAddress, clock.nowUTC()))
                .setTaskId(Protos.TaskID.newBuilder().setValue(taskId(offer, clock)))
                .setSlaveId(offer.getSlaveId())
                .addAllResources(resources)
                .setDiscovery(discovery)
                .setCommand(nativeCommand(configuration, new List<String>(), elasticSearchNodeId))
                .build();
    }

    private Protos.TaskInfo buildDockerTask(Protos.Offer offer, Configuration configuration, Clock clock, Long elasticSearchNodeId) {
        final List<Integer> ports = getPorts(offer, configuration);
        final List<Protos.Resource> resources = getResources(configuration, ports);
        final Protos.DiscoveryInfo discovery = getDiscovery(ports, configuration);

        final String hostAddress = resolveHostAddress(offer, ports);

        LOGGER.info("Creating Elasticsearch task with resources: " + resources.toString());

        final Protos.TaskID taskId = Protos.TaskID.newBuilder().setValue(taskId(offer, clock)).build();
        final Protos.ContainerInfo containerInfo = getContainer(configuration, taskId, elasticSearchNodeId, offer.getSlaveId());

        return Protos.TaskInfo.newBuilder()
                .setName(configuration.getTaskName())
                .setData(toData(offer.getHostname(), hostAddress, clock.nowUTC()))
                .setTaskId(taskId)
                .setSlaveId(offer.getSlaveId())
                .addAllResources(resources)
                .setDiscovery(discovery)
                .setCommand(dockerCommand(configuration, new List<String>(), elasticSearchNodeId))
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
        List<Integer> elasticsearchPorts = configuration.getElasticsearchPorts();
        if (elasticsearchPorts.isEmpty() || elasticsearchPorts.stream().allMatch(port -> port == 0)) {
            //No ports requested by user or two random ports requested
            ports = Resources.selectTwoPortsFromRange(offer.getResourcesList());
        }
        else {
            //Replace a user requested port 0 with a random port
            ports = elasticsearchPorts.stream().map(port -> port != 0 ?  port : Resources.selectOnePortFromRange(offer.getResourcesList())).collect(Collectors.toList());
        }
        return ports;
    }

    private List<Protos.Resource> getResources(Configuration configuration, List<Integer> ports) {
        List<Protos.Resource> acceptedResources = Resources.buildFrameworkResources(configuration);
        acceptedResources.add(Resources.singlePortRange(ports.get(0), configuration.getFrameworkRole()));
        acceptedResources.add(Resources.singlePortRange(ports.get(1), configuration.getFrameworkRole()));
        return acceptedResources;
    }

    private Protos.DiscoveryInfo getDiscovery(List<Integer> ports, Configuration configuration) {
        Protos.DiscoveryInfo.Builder discovery = Protos.DiscoveryInfo.newBuilder();
        Protos.Ports.Builder discoveryPorts = Protos.Ports.newBuilder();
        discoveryPorts.addPorts(Discovery.CLIENT_PORT_INDEX, Protos.Port.newBuilder().setNumber(ports.get(0)).setName(Discovery.CLIENT_PORT_NAME).setProtocol("TCP"));
        discoveryPorts.addPorts(Discovery.TRANSPORT_PORT_INDEX, Protos.Port.newBuilder().setNumber(ports.get(1)).setName(Discovery.TRANSPORT_PORT_NAME).setProtocol("TCP"));
        discovery.setPorts(discoveryPorts);
        discovery.setVisibility(Protos.DiscoveryInfo.Visibility.EXTERNAL);
        discovery.setName(configuration.getTaskName());
        return discovery.build();
    }

    private Protos.ContainerInfo getContainer(Configuration configuration, Protos.TaskID taskID, Long elasticSearchNodeId, Protos.SlaveID slaveID) {
        final Protos.Environment environment = Protos.Environment.newBuilder().addAllVariables(new ExecutorEnvironmentalVariables(configuration, elasticSearchNodeId).getList()).build();
        final Protos.ContainerInfo.DockerInfo.Builder dockerInfo = Protos.ContainerInfo.DockerInfo.newBuilder()
                .addParameters(Protos.Parameter.newBuilder().setKey("env").setValue("MESOS_TASK_ID=" + taskID.getValue()))
                .setImage(configuration.getExecutorImage())
                .setForcePullImage(configuration.getExecutorForcePullImage())
                .setNetwork(Protos.ContainerInfo.DockerInfo.Network.HOST);
        // Add all env vars to container
        for (Protos.Environment.Variable variable : environment.getVariablesList()) {
            dockerInfo.addParameters(Protos.Parameter.newBuilder().setKey("env").setValue(variable.getName() + "=" + variable.getValue()));
        }

        final Protos.ContainerInfo.Builder builder = Protos.ContainerInfo.newBuilder()
                .setType(Protos.ContainerInfo.Type.DOCKER);

        if (configuration.getExternalVolumeDriver() != null && configuration.getExternalVolumeDriver().length() > 0) {

            LOGGER.debug("Is Docker Container and External Driver enabled");

            //docker external volume driver
            LOGGER.debug("Docker Driver: " + configuration.getExternalVolumeDriver());

            //note: this makes a unique data volume name per elastic search node
            StringBuffer sbData = new StringBuffer(configuration.getFrameworkName());
            sbData.append(Long.toString(elasticSearchNodeId));
            sbData.append("data:");
            sbData.append(Configuration.CONTAINER_PATH_DATA);
            String sHostPathOrExternalVolumeForData = sbData.toString();
            LOGGER.debug("Data Volume Name: " + sHostPathOrExternalVolumeForData);

            dockerInfo.addParameters(Protos.Parameter.newBuilder()
                    .setKey("volume-driver")
                    .setValue(configuration.getExternalVolumeDriver()));
            dockerInfo.addParameters(Protos.Parameter.newBuilder()
                    .setKey("volume")
                    .setValue(sHostPathOrExternalVolumeForData));
        } else {
            if (!configuration.getDataDir().isEmpty()) {
                builder.addVolumes(Protos.Volume.newBuilder()
                        .setHostPath(configuration.taskSpecificHostDir(slaveID))
                        .setContainerPath(Configuration.CONTAINER_PATH_DATA)
                        .setMode(Protos.Volume.Mode.RW)
                        .build());
            }
        }

        builder.setDocker(dockerInfo);

        if (!configuration.getElasticsearchSettingsLocation().isEmpty()) {
            final Path path = Paths.get(configuration.getElasticsearchSettingsLocation());
            final Path fileName = path.getFileName();
            if (fileName == null) {
                throw new IllegalArgumentException("Cannot parse filename from settings location. Please include the /elasticsearch.yml in the settings location.");
            }
            final String settingsFilename = fileName.toString();
            // Mount the custom yml file over the top of the standard elasticsearch.yml file.
            builder.addVolumes(Protos.Volume.newBuilder()
                    .setHostPath("./" + settingsFilename) // Because the file has been uploaded by the uris.
                    .setContainerPath(Configuration.CONTAINER_PATH_CONF_YML)
                    .setMode(Protos.Volume.Mode.RO)
                    .build());
        }

        return builder
                .build();
    }

    private Protos.CommandInfo dockerCommand(Configuration configuration, List<String> args, Long elasticSearchNodeId) {
        final Protos.Environment environment = Protos.Environment.newBuilder().addAllVariables(new ExecutorEnvironmentalVariables(configuration, elasticSearchNodeId).getList()).build();
        final Protos.CommandInfo.Builder builder = Protos.CommandInfo.newBuilder()
                .setShell(false)
                .mergeEnvironment(environment)
                .addAllArguments(args);
        if (!configuration.getElasticsearchSettingsLocation().isEmpty()) {
            builder.addUris(Protos.CommandInfo.URI.newBuilder().setValue(configuration.getElasticsearchSettingsLocation()));
        }
        return builder
                .build();
    }

    private Protos.CommandInfo nativeCommand(Configuration configuration, List<String> args, Long elasticSearchNodeId) {
        String address = configuration.getFrameworkFileServerAddress();
        if (address == null) {
            throw new NullPointerException("Webserver address is null");
        }
        String httpPath = address + "/get/" + Configuration.ES_TAR;
        String command = configuration.nativeCommand(args);
        final Protos.Environment environment = Protos.Environment.newBuilder().addAllVariables(new ExecutorEnvironmentalVariables(configuration, elasticSearchNodeId).getList()).build();
        final Protos.CommandInfo.Builder builder = Protos.CommandInfo.newBuilder()
                .setShell(true)
                .setValue(command)
                .setUser("root")
                .mergeEnvironment(environment);
        if (configuration.getElasticsearchBinary().isEmpty()) {
            builder.addUris(Protos.CommandInfo.URI.newBuilder().setValue(httpPath));
        } else {
            builder.addUris(Protos.CommandInfo.URI.newBuilder().setValue(configuration.getElasticsearchBinary()));
        }
        if (!configuration.getElasticsearchSettingsLocation().isEmpty()) {
            builder.addUris(Protos.CommandInfo.URI.newBuilder().setValue(configuration.getElasticsearchSettingsLocation()));
        }
        return builder
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
