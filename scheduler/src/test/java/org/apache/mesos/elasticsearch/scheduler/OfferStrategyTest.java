package org.apache.mesos.elasticsearch.scheduler;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.scheduler.cluster.ESTask;
import org.apache.mesos.elasticsearch.scheduler.state.ClusterState;
import org.apache.mesos.elasticsearch.scheduler.state.ESTaskStatus;
import org.apache.mesos.elasticsearch.scheduler.util.ProtoTestUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;

import static java.util.Collections.singletonList;
import static org.apache.mesos.elasticsearch.scheduler.Resources.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Test offer strategy
 */
@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings("PMD.TooManyMethods")
public class OfferStrategyTest {

    @Mock
    Configuration configuration;

    @Mock
    ClusterState clusterState;

    @InjectMocks
    OfferStrategyNormal offerStrategy;

    @Before
    public void setUp() throws Exception {
        when(configuration.getFrameworkRole()).thenReturn("testRole");
        ESTaskStatus esTaskStatus = mock(ESTaskStatus.class);
        when(esTaskStatus.getStatus()).thenReturn(taskStatus());
    }
    
    @Test
    public void willDeclineIfHostIsAlreadyRunningTask() throws Exception {
        final ESTask esTask = mock(ESTask.class, RETURNS_DEEP_STUBS);
        when(esTask.getTask()).thenReturn(ProtoTestUtil.getDefaultTaskInfo());
        when(clusterState.get()).thenReturn(singletonList(esTask));

        final OfferStrategyNormal.OfferResult result = offerStrategy.evaluate(validOffer(ProtoTestUtil.getSlaveId().getValue()));
        assertFalse(result.acceptable);
        assertEquals("Host already running task", result.reason.get());
    }

    @Test
    public void willDeclineIfClusterSizeIsFulfilled() throws Exception {
        when(configuration.getElasticsearchNodes()).thenReturn(0);

        final OfferStrategyNormal.OfferResult result = offerStrategy.evaluate(baseOfferBuilder("host4")
                .addResources(portRange(31000, 32000, configuration.getFrameworkRole()))
                .addResources(portRange(9200, 9300, configuration.getFrameworkRole()))
                .addResources(cpus(configuration.getCpus(), configuration.getFrameworkRole()))
                .addResources(mem(configuration.getMem(), configuration.getFrameworkRole()))
                .addResources(disk(configuration.getDisk(), configuration.getFrameworkRole()))
                .build());
        assertFalse(result.acceptable);
        assertEquals("Cluster size already fulfilled", result.reason.get());
    }

    @Test
    public void willDeclineIfOfferDoesNotHaveTwoPorts() throws Exception {
        when(configuration.getElasticsearchNodes()).thenReturn(3);

        final OfferStrategyNormal.OfferResult offerResult = offerStrategy.evaluate(baseOfferBuilder("host3")
                .addResources(portRange(9200, 9200, configuration.getFrameworkRole()))
                .build());
        assertFalse(offerResult.acceptable);
        assertEquals("Offer did not have 2 ports", offerResult.reason.get());
    }

    @Test
    public void shouldDeclineIfUserPortsNotAvailable() throws Exception {
        when(configuration.getElasticsearchNodes()).thenReturn(3);
        when(configuration.getElasticsearchPorts()).thenReturn(Arrays.asList(9200, 9300));

        final OfferStrategyNormal.OfferResult offerResult = offerStrategy.evaluate(baseOfferBuilder("host3")
                .addResources(portRange(31000, 32000, configuration.getFrameworkRole()))
                .build());
        assertFalse(offerResult.acceptable);
        assertEquals("The offer does not contain the user specified ports", offerResult.reason.get());
    }

    @Test
    public void shouldDeclineIfOneOfUserPortsNotAvailable() throws Exception {
        when(configuration.getElasticsearchNodes()).thenReturn(3);
        when(configuration.getElasticsearchPorts()).thenReturn(Arrays.asList(31000, 9300));

        final OfferStrategyNormal.OfferResult offerResult = offerStrategy.evaluate(baseOfferBuilder("host3")
                .addResources(portRange(31000, 32000, configuration.getFrameworkRole()))
                .build());
        assertFalse(offerResult.acceptable);
        assertEquals("The offer does not contain the user specified ports", offerResult.reason.get());
    }

    @Test
    public void shouldAcceptIfUserPortsAreAvailable() throws Exception {
        when(configuration.getElasticsearchNodes()).thenReturn(3);
        when(configuration.getElasticsearchPorts()).thenReturn(Arrays.asList(9200, 9300));

        final OfferStrategyNormal.OfferResult offerResult = offerStrategy.evaluate(baseOfferBuilder("host3")
                .addResources(portRange(31000, 32000, configuration.getFrameworkRole()))
                .addResources(portRange(9200, 9300, configuration.getFrameworkRole()))
                .addResources(cpus(configuration.getCpus(), configuration.getFrameworkRole()))
                .addResources(mem(configuration.getMem(), configuration.getFrameworkRole()))
                .addResources(disk(configuration.getDisk(), configuration.getFrameworkRole()))
                .build());
        assertTrue(offerResult.acceptable);
    }

    @Test
    public void willDeclineIfOfferDoesNotEnoughCpu() throws Exception {
        when(configuration.getElasticsearchNodes()).thenReturn(3);
        when(configuration.getCpus()).thenReturn(1.0);

        final OfferStrategyNormal.OfferResult offerResult = offerStrategy.evaluate(baseOfferBuilder("host3")
                .addResources(portRange(9200, 9200, configuration.getFrameworkRole()))
                .addResources(portRange(9300, 9300, configuration.getFrameworkRole()))
                .addResources(cpus(0.1, configuration.getFrameworkRole()))
                .build());
        assertFalse(offerResult.acceptable);
        assertEquals("Offer did not have enough CPU resources", offerResult.reason.get());
    }
    @Test
    public void willDeclineIfOfferDoesNotEnoughMem() throws Exception {
        when(configuration.getElasticsearchNodes()).thenReturn(3);
        when(configuration.getMem()).thenReturn(100.0);

        final OfferStrategyNormal.OfferResult offerResult = offerStrategy.evaluate(baseOfferBuilder("host3")
                .addResources(portRange(9200, 9200, configuration.getFrameworkRole()))
                .addResources(portRange(9300, 9300, configuration.getFrameworkRole()))
                .addResources(cpus(10.0, configuration.getFrameworkRole()))
                .addResources(mem(10, configuration.getFrameworkRole()))
                .build());
        assertFalse(offerResult.acceptable);
        assertEquals("Offer did not have enough RAM resources", offerResult.reason.get());
    }

    @Test
    public void willDeclineIfOfferDoesNotEnoughDisk() throws Exception {
        when(configuration.getElasticsearchNodes()).thenReturn(3);
        when(configuration.getDisk()).thenReturn(100.0);

        final OfferStrategyNormal.OfferResult offerResult = offerStrategy.evaluate(baseOfferBuilder("host3")
                .addResources(portRange(9200, 9200, configuration.getFrameworkRole()))
                .addResources(portRange(9300, 9300, configuration.getFrameworkRole()))
                .addResources(cpus(10.0, configuration.getFrameworkRole()))
                .addResources(mem(100, configuration.getFrameworkRole()))
                .addResources(disk(10, configuration.getFrameworkRole()))
                .build());
        assertFalse(offerResult.acceptable);
        assertEquals("Offer did not have enough disk resources", offerResult.reason.get());
    }

    @Test
    public void willAcceptValidOffer() throws Exception {
        when(configuration.getElasticsearchNodes()).thenReturn(3);

        final OfferStrategyNormal.OfferResult offerResult = offerStrategy.evaluate(baseOfferBuilder("host3")
                .addResources(portRange(9200, 9200, configuration.getFrameworkRole()))
                .addResources(portRange(9300, 9300, configuration.getFrameworkRole()))
                .addResources(cpus(configuration.getCpus(), configuration.getFrameworkRole()))
                .addResources(mem(configuration.getMem(), configuration.getFrameworkRole()))
                .addResources(disk(configuration.getDisk(), configuration.getFrameworkRole()))
                .build());
        assertTrue(offerResult.acceptable);
        assertFalse(offerResult.reason.isPresent());
    }

    @Test
    public void shouldDeclineWhenHostIsUnresolveable() throws InvalidProtocolBufferException {
        when(configuration.getElasticsearchNodes()).thenReturn(3);
        final OfferStrategyNormal.OfferResult offerResult = offerStrategy.evaluate(baseOfferBuilder("host3")
                .addResources(portRange(9200, 9200, configuration.getFrameworkRole()))
                .addResources(portRange(9300, 9300, configuration.getFrameworkRole()))
                .addResources(cpus(configuration.getCpus(), configuration.getFrameworkRole()))
                .addResources(mem(configuration.getMem(), configuration.getFrameworkRole()))
                .addResources(disk(configuration.getDisk(), configuration.getFrameworkRole()))
                .setHostname("NonResolvableHostname")
                .build());
        assertFalse(offerResult.acceptable);
    }

    @Test
    public void shouldAcceptWhenHostIsResolveable() throws InvalidProtocolBufferException {
        when(configuration.getElasticsearchNodes()).thenReturn(3);
        final OfferStrategyNormal.OfferResult offerResult = offerStrategy.evaluate(baseOfferBuilder("host3")
                .addResources(portRange(9200, 9200, configuration.getFrameworkRole()))
                .addResources(portRange(9300, 9300, configuration.getFrameworkRole()))
                .addResources(cpus(configuration.getCpus(), configuration.getFrameworkRole()))
                .addResources(mem(configuration.getMem(), configuration.getFrameworkRole()))
                .addResources(disk(configuration.getDisk(), configuration.getFrameworkRole()))
                .build());
        assertTrue(offerResult.acceptable);
    }

    private Protos.TaskInfo createTask(String hostname) throws InvalidProtocolBufferException {
        return Protos.TaskInfo.newBuilder()
                .setName("Test")
                .setTaskId(Protos.TaskID.newBuilder().setValue("TestId").build())
                .setSlaveId(Protos.SlaveID.newBuilder().setValue(hostname).build())
                .build();
    }

    private Protos.Offer validOffer(String slaveId) {
        return baseOfferBuilder(slaveId)
                .build();
    }

    private Protos.Offer.Builder baseOfferBuilder(String slaveId) {
        return Protos.Offer.newBuilder()
                .setId(Protos.OfferID.newBuilder().setValue("offerId").build())
                .setFrameworkId(Protos.FrameworkID.newBuilder().setValue("testframework").build())
                .setHostname("localhost")
                .setSlaveId(Protos.SlaveID.newBuilder().setValue(slaveId).build());
    }

    private Protos.TaskStatus taskStatus() {
        return Protos.TaskStatus.newBuilder()
                .setTaskId(Protos.TaskID.newBuilder().setValue("TestId").build())
                .setState(Protos.TaskState.TASK_RUNNING)
                .build();
    }
}