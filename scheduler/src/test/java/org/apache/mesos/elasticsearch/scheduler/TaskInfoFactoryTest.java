package org.apache.mesos.elasticsearch.scheduler;

import org.apache.mesos.Protos;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.Date;
import java.util.UUID;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests TaskInfoFactory
 */
public class TaskInfoFactoryTest {

    private static final double EPSILON = 0.0001;

    @Test
    public void testCreateTaskInfo() {
        Clock clock = mock(Clock.class);

        TaskInfoFactory factory = new TaskInfoFactory();
        factory.clock = clock;

        Date now = new DateTime().withDayOfMonth(1).withDayOfYear(1).withYear(1970).withHourOfDay(1).withMinuteOfHour(2).withSecondOfMinute(3).withMillisOfSecond(400).toDate();
        when(clock.now()).thenReturn(now);

        Protos.FrameworkID frameworkId = Protos.FrameworkID.newBuilder().setValue(UUID.randomUUID().toString()).build();

        Configuration configuration = mock(Configuration.class);
        when(configuration.getFrameworkId()).thenReturn(frameworkId);
        when(configuration.getTaskName()).thenReturn("esdemo");
        when(configuration.getMesosZKURL()).thenReturn("zk://zookeeper:2181/mesos");
        when(configuration.getFrameworkZKURL()).thenReturn("zk://zookeeper:2181/mesos");
        when(configuration.getEexecutorImage()).thenReturn(Configuration.DEFAULT_EXECUTOR_IMAGE);
        when(configuration.getElasticsearchSettingsLocation()).thenReturn("/var");
        when(configuration.getElasticsearchNodes()).thenReturn(3);
        when(configuration.getElasticsearchClusterName()).thenReturn("cluster-name");
        when(configuration.getDataDir()).thenReturn("/var/lib/mesos/slave/elasticsearch");

        Protos.Offer offer = Protos.Offer.newBuilder()
                                            .setId(Protos.OfferID.newBuilder().setValue(UUID.randomUUID().toString()))
                                            .setSlaveId(Protos.SlaveID.newBuilder().setValue(UUID.randomUUID().toString()))
                                            .setFrameworkId(frameworkId)
                                            .setHostname("host1")
                                            .addAllResources(asList(
                                                    Resources.singlePortRange(9200),
                                                    Resources.singlePortRange(9300),
                                                    Resources.cpus(1.0),
                                                    Resources.disk(2.0),
                                                    Resources.mem(3.0)))
                                        .build();

        Protos.TaskInfo taskInfo = factory.createTask(configuration, offer);

        assertEquals(configuration.getTaskName(), taskInfo.getName());
        assertEquals(offer.getSlaveId(), taskInfo.getSlaveId());
        assertEquals("elasticsearch_host1_19700101T010203.400Z", taskInfo.getTaskId().getValue());

        // TODO: Should get resources by name, not by index. Position is arbitrary.
        assertEquals("cpus", taskInfo.getResources(0).getName());
        assertEquals(configuration.getCpus(), taskInfo.getResources(0).getScalar().getValue(), EPSILON);

        assertEquals("disk", taskInfo.getResources(2).getName());
        assertEquals(configuration.getDisk(), taskInfo.getResources(2).getScalar().getValue(), EPSILON);

        assertEquals("mem", taskInfo.getResources(1).getName());
        assertEquals(configuration.getMem(), taskInfo.getResources(1).getScalar().getValue(), EPSILON);

        assertEquals("ports", taskInfo.getResources(3).getName());
        assertEquals(9200, taskInfo.getResources(3).getRanges().getRange(0).getBegin());
        assertEquals(9200, taskInfo.getResources(3).getRanges().getRange(0).getEnd());

        assertEquals("ports", taskInfo.getResources(4).getName());
        assertEquals(9300, taskInfo.getResources(4).getRanges().getRange(0).getBegin());
        assertEquals(9300, taskInfo.getResources(4).getRanges().getRange(0).getEnd());

        assertEquals(9200, taskInfo.getDiscovery().getPorts().getPorts(0).getNumber());
        assertEquals(9300, taskInfo.getDiscovery().getPorts().getPorts(1).getNumber());
        assertEquals(Protos.DiscoveryInfo.Visibility.EXTERNAL, taskInfo.getDiscovery().getVisibility());

        assertEquals(configuration.getFrameworkId(), taskInfo.getExecutor().getFrameworkId());
        assertEquals(Configuration.DEFAULT_EXECUTOR_IMAGE, taskInfo.getExecutor().getContainer().getDocker().getImage());

        assertEquals(2, taskInfo.getExecutor().getContainer().getVolumesCount());
        assertEquals(TaskInfoFactory.SETTINGS_PATH_VOLUME, taskInfo.getExecutor().getContainer().getVolumes(0).getContainerPath());
        assertEquals(TaskInfoFactory.SETTINGS_PATH_VOLUME, taskInfo.getExecutor().getContainer().getVolumes(0).getHostPath());
        assertEquals(Protos.Volume.Mode.RO, taskInfo.getExecutor().getContainer().getVolumes(0).getMode());
        assertEquals(TaskInfoFactory.SETTINGS_DATA_VOLUME_CONTAINER, taskInfo.getExecutor().getContainer().getVolumes(1).getContainerPath());
        assertEquals(Configuration.DEFAULT_HOST_DATA_DIR, taskInfo.getExecutor().getContainer().getVolumes(1).getHostPath());
        assertEquals(Protos.Volume.Mode.RW, taskInfo.getExecutor().getContainer().getVolumes(1).getMode());
    }
}
