package org.apache.mesos.elasticsearch.scheduler;

import org.apache.log4j.Logger;
import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.common.Discovery;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;

/**
 * Factory for creating {@link Protos.TaskInfo}s
 */
public class TaskInfoFactory {

    public static final Logger LOGGER = Logger.getLogger(ElasticsearchScheduler.class.toString());

    public static final String TASK_DATE_FORMAT = "yyyyMMdd'T'HHmmss.SSS'Z'";

    Clock clock = new Clock();

    @SuppressWarnings("PMD.ExcessiveMethodLength")
    public Protos.TaskInfo createTask(Protos.Offer offer, Protos.FrameworkID frameworkId, org.apache.mesos.elasticsearch.scheduler.Configuration configuration) {
        List<Integer> ports = Resources.selectTwoPortsFromRange(offer.getResourcesList());

        List<Protos.Resource> acceptedResources = new ArrayList<>();

        addAllScalarResources(offer.getResourcesList(), acceptedResources);

        LOGGER.info("Elasticsearch client port " + ports.get(0));
        LOGGER.info("Elasticsearch transport port " + ports.get(1));

        Protos.DiscoveryInfo.Builder discovery = Protos.DiscoveryInfo.newBuilder();
        Protos.Ports.Builder discoveryPorts = Protos.Ports.newBuilder();
        acceptedResources.add(Resources.singlePortRange(ports.get(0)));
        acceptedResources.add(Resources.singlePortRange(ports.get(1)));
        discoveryPorts.addPorts(Discovery.CLIENT_PORT_INDEX, Protos.Port.newBuilder().setNumber(ports.get(0)).setName(Discovery.CLIENT_PORT_NAME));
        discoveryPorts.addPorts(Discovery.TRANSPORT_PORT_INDEX, Protos.Port.newBuilder().setNumber(ports.get(1)).setName(Discovery.TRANSPORT_PORT_NAME));
        discovery.setPorts(discoveryPorts);
        discovery.setVisibility(Protos.DiscoveryInfo.Visibility.EXTERNAL);

        Protos.TaskInfo.Builder taskInfoBuilder = Protos.TaskInfo.newBuilder()
                .setName(configuration.getTaskName())
                .setTaskId(Protos.TaskID.newBuilder().setValue(taskId(offer)))
                .setSlaveId(offer.getSlaveId())
                .addAllResources(acceptedResources)
                .setDiscovery(discovery);

        Protos.Volume volume = Protos.Volume.newBuilder().setContainerPath("/usr/lib").setHostPath("/usr/lib").setMode(org.apache.mesos.Protos.Volume.Mode.RO).build();

        Protos.ContainerInfo.DockerInfo dockerInfo = Protos.ContainerInfo.DockerInfo.newBuilder().setImage("mesos/elasticsearch-executor").build();

        Protos.ContainerInfo containerInfo = Protos.ContainerInfo.newBuilder()
                .setDocker(dockerInfo)
                .setType(org.apache.mesos.Protos.ContainerInfo.Type.DOCKER)
                .addVolumes(volume)
                .build();

        Protos.CommandInfo commandInfo = Protos.CommandInfo.newBuilder()
                .setValue("java -Djava.library.path=/usr/lib -jar /tmp/elasticsearch-mesos-executor.jar")
                .addAllArguments(asList("-zk", configuration.getZookeeperHost())).build();

        Protos.ExecutorInfo.Builder executorInfo = Protos.ExecutorInfo.newBuilder()
                .setContainer(containerInfo)
                .setCommand(commandInfo)
                .setExecutorId(Protos.ExecutorID.newBuilder().setValue(UUID.randomUUID().toString()))
                .setFrameworkId(configuration.getFrameworkId())
                .setName(UUID.randomUUID().toString());

        taskInfoBuilder.setExecutor(executorInfo);

        return taskInfoBuilder.build();
    }

    private void addAllScalarResources(List<Protos.Resource> offeredResources, List<Protos.Resource> acceptedResources) {
        acceptedResources.addAll(offeredResources.stream().filter(resource -> resource.getType().equals(org.apache.mesos.Protos.Value.Type.SCALAR)).collect(Collectors.toList()));
    }

    private String taskId(Protos.Offer offer) {
        String date = new SimpleDateFormat(TASK_DATE_FORMAT).format(clock.now());
        return String.format("elasticsearch_%s_%s", offer.getHostname(), date);
    }

}
