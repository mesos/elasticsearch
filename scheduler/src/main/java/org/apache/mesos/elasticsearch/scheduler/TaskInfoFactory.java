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

    public static final Logger LOGGER = Logger.getLogger(TaskInfoFactory.class);

    public static final String TASK_DATE_FORMAT = "yyyyMMdd'T'HHmmss.SSS'Z'";

    Clock clock = new Clock();

    @SuppressWarnings("PMD.ExcessiveMethodLength")
    public Protos.TaskInfo createTask(Configuration configuration, Protos.Offer offer) {
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

        // .setShell(true).setValue("java -Djava.library.path=/usr/lib -jar /tmp/mesos-elasticsearch-executor.jar")
        // .setCommand(Protos.CommandInfo.newBuilder().setShell(false).addAllArguments(asList("-zk", configuration.getZookeeperHost())))

        return Protos.TaskInfo.newBuilder()
                .setName(configuration.getTaskName())
                .setTaskId(Protos.TaskID.newBuilder().setValue(taskId(offer)))
                .setSlaveId(offer.getSlaveId())
                .addAllResources(acceptedResources)
                .setDiscovery(discovery)
                .setExecutor(newExecutorInfo(configuration, acceptedResources)).build();
    }

    private Protos.ExecutorInfo.Builder newExecutorInfo(Configuration configuration, List<Protos.Resource> resources) {
        return Protos.ExecutorInfo.newBuilder()
                .setExecutorId(Protos.ExecutorID.newBuilder().setValue(UUID.randomUUID().toString()))
                .setFrameworkId(configuration.getFrameworkId())
                .setName(UUID.randomUUID().toString())
                .addAllResources(resources)
                .setCommand(newCommandInfo(configuration))
                .setContainer(Protos.ContainerInfo.newBuilder()
                        .setType(Protos.ContainerInfo.Type.DOCKER)
                        .setDocker(Protos.ContainerInfo.DockerInfo.newBuilder().setImage("mesos/elasticsearch-executor"))
                        .addVolumes(Protos.Volume.newBuilder().setContainerPath("/usr/lib").setHostPath("/usr/lib").setMode(Protos.Volume.Mode.RO).build())
                        .build());
    }

    private Protos.CommandInfo.Builder newCommandInfo(Configuration configuration) {
        return Protos.CommandInfo.newBuilder()
                .addAllArguments(asList("-zk", configuration.getZookeeperHost()))
                .setContainer(Protos.CommandInfo.ContainerInfo.newBuilder().setImage("mesos/elasticsearch-executor").build());
    }

    private void addAllScalarResources(List<Protos.Resource> offeredResources, List<Protos.Resource> acceptedResources) {
        acceptedResources.addAll(offeredResources.stream().filter(resource -> resource.getType().equals(org.apache.mesos.Protos.Value.Type.SCALAR))
                .map(resource -> resource = Protos.Resource.newBuilder().setType(Protos.Value.Type.SCALAR).setName(resource.getName()).setScalar(Protos.Value.Scalar.newBuilder().setValue(resource.getScalar().getValue() * 0.5)).build())
                .collect(Collectors.toList()));
    }

    private String taskId(Protos.Offer offer) {
        String date = new SimpleDateFormat(TASK_DATE_FORMAT).format(clock.now());
        return String.format("elasticsearch_%s_%s", offer.getHostname(), date);
    }

}
