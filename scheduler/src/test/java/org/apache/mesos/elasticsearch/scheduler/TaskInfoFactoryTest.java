package org.apache.mesos.elasticsearch.scheduler;

import org.apache.mesos.Protos;
import org.junit.Test;

import java.util.UUID;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests TaskInfoFactory
 */
public class TaskInfoFactoryTest {

    @Test
    public void testCreateTaskInfo() {
        TaskInfoFactory factory = new TaskInfoFactory();

        Protos.FrameworkID frameworkId = Protos.FrameworkID.newBuilder().setValue(UUID.randomUUID().toString()).build();

        Configuration configuration = mock(Configuration.class);
        when(configuration.getFrameworkId()).thenReturn(frameworkId);
        when(configuration.getTaskName()).thenReturn("esdemo");
        when(configuration.getZookeeperHost()).thenReturn("zookeeper");

        Protos.Offer offer = Protos.Offer.newBuilder()
                                            .setId(Protos.OfferID.newBuilder().setValue(UUID.randomUUID().toString()))
                                            .setSlaveId(Protos.SlaveID.newBuilder().setValue(UUID.randomUUID().toString()))
                                            .setFrameworkId(frameworkId)
                                            .setHostname("host1")
                                            .addAllResources(asList(Resources.singlePortRange(9200), Resources.singlePortRange(9300)))
                                        .build();

        Protos.TaskInfo taskInfo = factory.createTask(configuration, offer);

        assertEquals(configuration.getTaskName(), taskInfo.getName());
    }
}
