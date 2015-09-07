package org.apache.mesos.elasticsearch.scheduler;

import com.google.protobuf.ByteString;
import org.apache.log4j.Logger;
import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.common.Discovery;
import org.apache.mesos.elasticsearch.common.cli.ElasticsearchCLIParameter;
import org.apache.mesos.elasticsearch.common.cli.ZookeeperCLIParameter;
import org.apache.mesos.elasticsearch.scheduler.configuration.ExecutorEnvironmentalVariables;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static java.util.Arrays.asList;

/**
 * Factory for creating {@link Protos.TaskInfo}s
 */
public class TaskInfoFactory {

    private static final Logger LOGGER = Logger.getLogger(TaskInfoFactory.class);

    public static final String TASK_DATE_FORMAT = "yyyyMMdd'T'HHmmss.SSS'Z'";

    public static final String SETTINGS_PATH_VOLUME = "/tmp/config";

    public static final String SETTINGS_DATA_VOLUME_CONTAINER = "/data";

    Clock clock = new Clock();

    /**
     * Creates TaskInfo for Elasticsearch execcutor running in a Docker container
     *
     * @param configuration configuation of the framework
     * @param offer with resources to run the executor with
     *
     * @return TaskInfo
     */
    public Protos.TaskInfo createTask(Configuration configuration, Protos.Offer offer) {
        List<Integer> ports = Resources.selectTwoPortsFromRange(offer.getResourcesList());

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

        return Protos.TaskInfo.newBuilder()
                .setName(configuration.getTaskName())
                .setData(toData(offer.getHostname(), new InetSocketAddress(offer.getHostname(), 1).getAddress().getHostAddress(), clock.zonedNow()))
                .setTaskId(Protos.TaskID.newBuilder().setValue(taskId(offer)))
                .setSlaveId(offer.getSlaveId())
                .addAllResources(acceptedResources)
                .setDiscovery(discovery)
                .setExecutor(newExecutorInfo(configuration)).build();
    }

    private ByteString toData(String hostname, String ipAddress, ZonedDateTime zonedDateTime) {
        Properties data = new Properties();
        data.put("hostname", hostname);
        data.put("ipAddress", ipAddress);
        data.put("startedAt", zonedDateTime.toString());

        StringWriter writer = new StringWriter();
        data.list(new PrintWriter(writer));
        return ByteString.copyFromUtf8(writer.getBuffer().toString());
    }

    private Protos.ExecutorInfo.Builder newExecutorInfo(Configuration configuration) {
        return Protos.ExecutorInfo.newBuilder()
                .setExecutorId(Protos.ExecutorID.newBuilder().setValue(UUID.randomUUID().toString()))
                .setFrameworkId(configuration.getFrameworkId())
                .setName("elasticsearch-executor-" + UUID.randomUUID().toString())
                .setCommand(newCommandInfo(configuration))
                .setContainer(Protos.ContainerInfo.newBuilder()
                        .setType(Protos.ContainerInfo.Type.DOCKER)
                        .setDocker(Protos.ContainerInfo.DockerInfo.newBuilder().setImage(configuration.getExecutorImage()).setForcePullImage(configuration.getExecutorForcePullImage()))
                        .addVolumes(Protos.Volume.newBuilder().setHostPath(SETTINGS_PATH_VOLUME).setContainerPath(SETTINGS_PATH_VOLUME).setMode(Protos.Volume.Mode.RO)) // Temporary fix until we get a data container.
                        .addVolumes(Protos.Volume.newBuilder().setContainerPath(SETTINGS_DATA_VOLUME_CONTAINER).setHostPath(configuration.getDataDir()).setMode(Protos.Volume.Mode.RW).build())
                        .build());
    }

    private Protos.CommandInfo.Builder newCommandInfo(Configuration configuration) {
        ExecutorEnvironmentalVariables executorEnvironmentalVariables = new ExecutorEnvironmentalVariables(configuration);
        List<String> args = new ArrayList<>(
                asList(
                        ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, configuration.getMesosZKURL(),
                        ZookeeperCLIParameter.ZOOKEEPER_FRAMEWORK_URL, "zk://" + configuration.getFrameworkZKURL(), // Make framework url a valid url again.
                        ZookeeperCLIParameter.ZOOKEEPER_FRAMEWORK_TIMEOUT, String.valueOf(configuration.getFrameworkZKTimeout())
                ));
        addIfNotEmpty(args, ElasticsearchCLIParameter.ELASTICSEARCH_SETTINGS_LOCATION, configuration.getElasticsearchSettingsLocation());
        addIfNotEmpty(args, ElasticsearchCLIParameter.ELASTICSEARCH_CLUSTER_NAME, configuration.getElasticsearchClusterName());
        args.addAll(asList(ElasticsearchCLIParameter.ELASTICSEARCH_NODES, Integer.toString(configuration.getElasticsearchNodes())));
        return Protos.CommandInfo.newBuilder()
                .setShell(false)
                .addAllArguments(args)
                .setEnvironment(Protos.Environment.newBuilder().addAllVariables(executorEnvironmentalVariables.getList()))
                .setContainer(Protos.CommandInfo.ContainerInfo.newBuilder().setImage(configuration.getExecutorImage()).build());
    }

    private void addIfNotEmpty(List<String> args, String key, String value) {
        if (!value.isEmpty()) {
            args.addAll(asList(key, value));
        }
    }

    private String taskId(Protos.Offer offer) {
        String date = new SimpleDateFormat(TASK_DATE_FORMAT).format(clock.now());
        return String.format("elasticsearch_%s_%s", offer.getHostname(), date);
    }

}
