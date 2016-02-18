package org.apache.mesos.elasticsearch.scheduler;

import com.google.protobuf.ByteString;
import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.common.Discovery;
import org.apache.mesos.elasticsearch.scheduler.state.ClusterState;
import org.apache.mesos.elasticsearch.scheduler.state.FrameworkState;
import org.apache.mesos.elasticsearch.scheduler.util.Clock;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Tests TaskInfoFactory
 */
@RunWith(MockitoJUnitRunner.class)
public class TaskInfoFactoryTest {

    private static final double EPSILON = 0.0001;

    @Mock
    private FrameworkState frameworkState;

    @Mock
    private ClusterState clusterState;

    @Mock
    private Configuration configuration;

    @Mock
    private Clock clock;

    @Before
    public void before() {
        Protos.FrameworkID frameworkId = Protos.FrameworkID.newBuilder().setValue(UUID.randomUUID().toString()).build();
        when(frameworkState.getFrameworkID()).thenReturn(frameworkId);
        when(configuration.getTaskName()).thenReturn("esdemo");
        when(configuration.getMesosZKURL()).thenReturn("zk://zookeeper:2181/mesos");
        when(configuration.getExecutorImage()).thenReturn(Configuration.DEFAULT_EXECUTOR_IMAGE);
        when(configuration.getElasticsearchSettingsLocation()).thenReturn(Configuration.HOST_PATH_CONF);
        when(configuration.getElasticsearchNodes()).thenReturn(3);
        when(configuration.getElasticsearchClusterName()).thenReturn("cluster-name");
        when(configuration.getDataDir()).thenReturn(Configuration.DEFAULT_HOST_DATA_DIR);
        when(configuration.getFrameworkRole()).thenReturn("some-framework-role");
        when(configuration.isFrameworkUseDocker()).thenReturn(true);
        when(configuration.getElasticsearchPorts()).thenReturn(Collections.emptyList());
    }

    @Test
    public void testCreateTaskInfo() {
        TaskInfoFactory factory = new TaskInfoFactory(clusterState);

        Date now = new DateTime().withDayOfMonth(1).withDayOfYear(1).withYear(1970).withHourOfDay(1).withMinuteOfHour(2).withSecondOfMinute(3).withMillisOfSecond(400).toDate();
        when(clock.now()).thenReturn(now);
        when(clock.nowUTC()).thenReturn(ZonedDateTime.now(ZoneOffset.UTC));


        Protos.Offer offer = getOffer(frameworkState.getFrameworkID());

        Protos.TaskInfo taskInfo = factory.createTask(configuration, frameworkState, offer, clock);

        assertEquals(configuration.getTaskName(), taskInfo.getName());
        assertEquals(offer.getSlaveId(), taskInfo.getSlaveId());
        assertEquals("elasticsearch_localhost_19700101T010203.400Z", taskInfo.getTaskId().getValue());

        List<Protos.Resource> resourceList = taskInfo.getResourcesList();
        assertEquals(configuration.getCpus(), getResourceByName(resourceList, "cpus").getScalar().getValue(), EPSILON);

        assertEquals(configuration.getDisk(), getResourceByName(resourceList, "disk").getScalar().getValue(), EPSILON);

        assertEquals(configuration.getMem(), getResourceByName(resourceList, "mem").getScalar().getValue(), EPSILON);

        List<Protos.Resource> portsList = getResourceListByName(resourceList, "ports");
        assertEquals(9200, portsList.get(0).getRanges().getRange(0).getBegin());
        assertEquals(9200, portsList.get(0).getRanges().getRange(0).getEnd());

        assertEquals(9300, portsList.get(1).getRanges().getRange(0).getBegin());
        assertEquals(9300, portsList.get(1).getRanges().getRange(0).getEnd());

        assertEquals(9200, taskInfo.getDiscovery().getPorts().getPorts(0).getNumber());
        assertEquals(9300, taskInfo.getDiscovery().getPorts().getPorts(1).getNumber());
        assertEquals(Protos.DiscoveryInfo.Visibility.EXTERNAL, taskInfo.getDiscovery().getVisibility());

        assertEquals(2, taskInfo.getContainer().getVolumesCount());
        assertEquals(Configuration.CONTAINER_PATH_DATA, taskInfo.getContainer().getVolumes(0).getContainerPath());
        assertEquals(Configuration.DEFAULT_HOST_DATA_DIR, taskInfo.getContainer().getVolumes(0).getHostPath());
        assertEquals(Protos.Volume.Mode.RW, taskInfo.getContainer().getVolumes(0).getMode());
        assertEquals(Configuration.CONTAINER_PATH_CONF, taskInfo.getContainer().getVolumes(1).getContainerPath());
        assertEquals(Configuration.HOST_PATH_CONF, taskInfo.getContainer().getVolumes(1).getHostPath());
        assertEquals(Protos.Volume.Mode.RO, taskInfo.getContainer().getVolumes(1).getMode());
    }

    private Protos.Resource getResourceByName(List<Protos.Resource> resourceList, String name) {
        return resourceList.stream().filter(resource -> resource.getName().equals(name)).findFirst().get();
    }

    private List<Protos.Resource> getResourceListByName(List<Protos.Resource> resourceList, String name) {
        return resourceList.stream().filter(resource -> resource.getName().equals(name)).collect(Collectors.toList());
    }

    private Protos.Offer getOffer(Protos.FrameworkID frameworkId) {
        return Protos.Offer.newBuilder()
                                                .setId(Protos.OfferID.newBuilder().setValue(UUID.randomUUID().toString()))
                                                .setSlaveId(Protos.SlaveID.newBuilder().setValue(UUID.randomUUID().toString()))
                                                .setFrameworkId(frameworkId)
                                                .setHostname("localhost")
                                                .addAllResources(asList(
                                                        Resources.singlePortRange(9200, "some-framework-role"),
                                                        Resources.singlePortRange(9300, "some-framework-role"),
                                                        Resources.cpus(1.0, "some-framework-role"),
                                                        Resources.disk(2.0, "some-framework-role"),
                                                        Resources.mem(3.0, "some-framework-role")))
                                            .build();
    }

    @Test
    public void shouldAddJarInfoAndRemoveContainerInfo() {
        when(configuration.isFrameworkUseDocker()).thenReturn(false);
        String address = "http://localhost:1234";
        when(configuration.getFrameworkFileServerAddress()).thenReturn(address);
        when(configuration.nativeCommand(any())).thenReturn("ls");
        TaskInfoFactory factory = new TaskInfoFactory(clusterState);

        Date now = new DateTime().withDayOfMonth(1).withDayOfYear(1).withYear(1970).withHourOfDay(1).withMinuteOfHour(2).withSecondOfMinute(3).withMillisOfSecond(400).toDate();
        when(clock.now()).thenReturn(now);
        when(clock.nowUTC()).thenReturn(ZonedDateTime.now(ZoneOffset.UTC));

        Protos.TaskInfo taskInfo = factory.createTask(configuration, frameworkState, getOffer(frameworkState.getFrameworkID()), clock);
        assertFalse(taskInfo.getContainer().isInitialized());
        assertTrue(taskInfo.getExecutor().getCommand().isInitialized());
        assertEquals(2, taskInfo.getCommand().getUrisCount());
        assertTrue(taskInfo.getCommand().getUris(0).getValue().contains(address));
    }

    @Test
    public void canParseTask() throws Exception {
        final ZonedDateTime nowUTC = ZonedDateTime.now(ZoneOffset.UTC);
        when(clock.nowUTC()).thenReturn(nowUTC.minusYears(100));

        final Protos.TaskID taskId = Protos.TaskID.newBuilder().setValue("TaskID").build();

        final Protos.TaskInfo taskInfo = createTaskInfo(taskId, createData(Optional.of(nowUTC)));

        final Protos.TaskStatus taskStatus = Protos.TaskStatus.newBuilder()
                .setTaskId(taskId)
                .setState(Protos.TaskState.TASK_STAGING)
                .build();

        final Task task = TaskInfoFactory.parse(taskInfo, taskStatus, clock);
        assertEquals(nowUTC, task.getStartedAt());
        verify(clock, never()).nowUTC();
    }

    @Test
    public void canParseTaskWithoutTimestamp() throws Exception {
        final ZonedDateTime nowUTC = ZonedDateTime.now(ZoneOffset.UTC);
        when(clock.nowUTC()).thenReturn(nowUTC);

        final Protos.TaskID taskId = Protos.TaskID.newBuilder().setValue("TaskID").build();

        final Protos.TaskInfo taskInfo = createTaskInfo(taskId, createData(Optional.<ZonedDateTime>empty()));

        final Protos.TaskStatus taskStatus = Protos.TaskStatus.newBuilder()
                .setTaskId(taskId)
                .setState(Protos.TaskState.TASK_STAGING)
                .build();

        final Task task = TaskInfoFactory.parse(taskInfo, taskStatus, clock);
        assertEquals(nowUTC, task.getStartedAt());
    }

    @Test
    public void canParseTaskCappedTimestamp() throws Exception {
        final ZonedDateTime nowUTC = ZonedDateTime.now(ZoneOffset.UTC);
        when(clock.nowUTC()).thenReturn(nowUTC);

        final Protos.TaskID taskId = Protos.TaskID.newBuilder().setValue("TaskID").build();

        Properties data = new Properties();
        data.put("hostname", "hostname");
        data.put("ipAddress", "ip address");
        data.put("startedAt", nowUTC.withZoneSameInstant(ZoneId.of("Europe/Paris")).toString());

        StringWriter writer = new StringWriter();
        data.list(new PrintWriter(writer));

        final Protos.TaskInfo taskInfo = createTaskInfo(taskId, ByteString.copyFromUtf8(writer.getBuffer().toString()));

        final Protos.TaskStatus taskStatus = Protos.TaskStatus.newBuilder()
                .setTaskId(taskId)
                .setState(Protos.TaskState.TASK_STAGING)
                .build();

        final Task task = TaskInfoFactory.parse(taskInfo, taskStatus, clock);
        assertEquals(nowUTC, task.getStartedAt());
    }

    @Test
    public void shouldAllowUserSpecifiedPorts() {
        when(configuration.getElasticsearchPorts()).thenReturn(Arrays.asList(123, 456));
        TaskInfoFactory factory = new TaskInfoFactory(clusterState);
        Protos.TaskInfo taskInfo = factory.createTask(configuration, frameworkState, getOffer(frameworkState.getFrameworkID()), new Clock());
        assertTrue(taskInfo.isInitialized());
        assertTrue(taskInfo.toString().contains("123"));
        assertTrue(taskInfo.toString().contains("456"));
    }

    @Test
    public void shouldUseMesosProvidedPorts() {
        TaskInfoFactory factory = new TaskInfoFactory(clusterState);
        Protos.TaskInfo taskInfo = factory.createTask(configuration, frameworkState, getOffer(frameworkState.getFrameworkID()), new Clock());
        assertTrue(taskInfo.getContainer().isInitialized());
        assertTrue(taskInfo.isInitialized());
        assertTrue(taskInfo.toString().contains("9200"));
        assertTrue(taskInfo.toString().contains("9300"));
    }

    private Protos.TaskInfo createTaskInfo(Protos.TaskID taskId, ByteString data) {
        Protos.DiscoveryInfo.Builder discovery = Protos.DiscoveryInfo.newBuilder()
                .setPorts(Protos.Ports.newBuilder()
                        .addPorts(Discovery.CLIENT_PORT_INDEX, Protos.Port.newBuilder().setNumber(9001).setName(Discovery.CLIENT_PORT_NAME))
                        .addPorts(Discovery.TRANSPORT_PORT_INDEX, Protos.Port.newBuilder().setNumber(9002).setName(Discovery.TRANSPORT_PORT_NAME)))
                .setVisibility(Protos.DiscoveryInfo.Visibility.EXTERNAL);

        return Protos.TaskInfo.newBuilder()
                .setName("Name")
                .setTaskId(taskId)
                .setSlaveId(Protos.SlaveID.newBuilder().setValue("SlaveID").build())
                .setData(data)
                .setDiscovery(discovery)
                .build();
    }

    private ByteString createData(Optional<ZonedDateTime> startedAt) {
        Properties data = new Properties();
        data.put("hostname", "hostname");
        data.put("ipAddress", "ip address");
        if (startedAt.isPresent()) {
            data.put("startedAt", startedAt.get().toString());
        }

        StringWriter writer = new StringWriter();
        try {
            data.store(new PrintWriter(writer), "Data");
        } catch (IOException e) {
            e.printStackTrace();
        }

        return ByteString.copyFromUtf8(writer.getBuffer().toString());
    }

}
