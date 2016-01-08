package org.apache.mesos.elasticsearch.scheduler;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.scheduler.state.ClusterState;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.mesos.elasticsearch.scheduler.Resources.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

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
    OfferStrategy offerStrategy;

    @Before
    public void setUp() throws Exception {
        when(configuration.getFrameworkRole()).thenReturn("testRole");
    }

    @Test
    public void willDeclineIfHostIsAlreadyRunningTask() throws Exception {
        when(clusterState.getTaskList()).thenReturn(singletonList(createTask("host1")));

        final OfferStrategy.OfferResult result = offerStrategy.evaluate(validOffer("host1"));
        assertFalse(result.acceptable);
        assertEquals("Host already running task", result.reason.get());
    }

    @Test
    public void willDeclineIfClusterSizeIsFulfilled() throws Exception {
        when(clusterState.getTaskList()).thenReturn(asList(createTask("host1"), createTask("host2"), createTask("host3")));
        when(configuration.getElasticsearchNodes()).thenReturn(3);

        final OfferStrategy.OfferResult result = offerStrategy.evaluate(validOffer("host4"));
        assertFalse(result.acceptable);
        assertEquals("Cluster size already fulfilled", result.reason.get());
    }

    @Test
    public void willDeclineIfOfferDoesNotHaveTwoPorts() throws Exception {
        when(clusterState.getTaskList()).thenReturn(asList(createTask("host1"), createTask("host2")));
        when(configuration.getElasticsearchNodes()).thenReturn(3);

        final OfferStrategy.OfferResult offerResult = offerStrategy.evaluate(baseOfferBuilder("host3")
                .addResources(portRange(9200, 9200, configuration.getFrameworkRole()))
                .build());
        assertFalse(offerResult.acceptable);
        assertEquals("Offer did not have 2 ports", offerResult.reason.get());
    }

    @Test
    public void shouldDeclineIfUserPortsNotAvailable() throws Exception {
        when(clusterState.getTaskList()).thenReturn(asList(createTask("host1"), createTask("host2")));
        when(configuration.getElasticsearchNodes()).thenReturn(3);
        when(configuration.getElasticsearchPorts()).thenReturn(Arrays.asList(9200, 9300));

        final OfferStrategy.OfferResult offerResult = offerStrategy.evaluate(baseOfferBuilder("host3")
                .addResources(portRange(31000, 32000, configuration.getFrameworkRole()))
                .build());
        assertFalse(offerResult.acceptable);
        assertEquals("The offer does not contain the user specified ports", offerResult.reason.get());
    }

    @Test
    public void shouldDeclineIfOneOfUserPortsNotAvailable() throws Exception {
        when(clusterState.getTaskList()).thenReturn(asList(createTask("host1"), createTask("host2")));
        when(configuration.getElasticsearchNodes()).thenReturn(3);
        when(configuration.getElasticsearchPorts()).thenReturn(Arrays.asList(31000, 9300));

        final OfferStrategy.OfferResult offerResult = offerStrategy.evaluate(baseOfferBuilder("host3")
                .addResources(portRange(31000, 32000, configuration.getFrameworkRole()))
                .build());
        assertFalse(offerResult.acceptable);
        assertEquals("The offer does not contain the user specified ports", offerResult.reason.get());
    }

    @Test
    public void shouldAcceptIfUserPortsAreAvailable() throws Exception {
        when(clusterState.getTaskList()).thenReturn(asList(createTask("host1"), createTask("host2")));
        when(configuration.getElasticsearchNodes()).thenReturn(3);
        when(configuration.getElasticsearchPorts()).thenReturn(Arrays.asList(9200, 9300));

        final OfferStrategy.OfferResult offerResult = offerStrategy.evaluate(baseOfferBuilder("host3")
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
        when(clusterState.getTaskList()).thenReturn(asList(createTask("host1"), createTask("host2")));
        when(configuration.getElasticsearchNodes()).thenReturn(3);
        when(configuration.getCpus()).thenReturn(1.0);

        final OfferStrategy.OfferResult offerResult = offerStrategy.evaluate(baseOfferBuilder("host3")
                .addResources(portRange(9200, 9200, configuration.getFrameworkRole()))
                .addResources(portRange(9300, 9300, configuration.getFrameworkRole()))
                .addResources(cpus(0.1, configuration.getFrameworkRole()))
                .build());
        assertFalse(offerResult.acceptable);
        assertEquals("Offer did not have enough CPU resources", offerResult.reason.get());
    }
    @Test
    public void willDeclineIfOfferDoesNotEnoughMem() throws Exception {
        when(clusterState.getTaskList()).thenReturn(asList(createTask("host1"), createTask("host2")));
        when(configuration.getElasticsearchNodes()).thenReturn(3);
        when(configuration.getMem()).thenReturn(100.0);

        final OfferStrategy.OfferResult offerResult = offerStrategy.evaluate(baseOfferBuilder("host3")
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
        when(clusterState.getTaskList()).thenReturn(asList(createTask("host1"), createTask("host2")));
        when(configuration.getElasticsearchNodes()).thenReturn(3);
        when(configuration.getDisk()).thenReturn(100.0);

        final OfferStrategy.OfferResult offerResult = offerStrategy.evaluate(baseOfferBuilder("host3")
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
        when(clusterState.getTaskList()).thenReturn(asList(createTask("host1"), createTask("host2")));
        when(configuration.getElasticsearchNodes()).thenReturn(3);

        final OfferStrategy.OfferResult offerResult = offerStrategy.evaluate(baseOfferBuilder("host3")
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
        when(clusterState.getTaskList()).thenReturn(asList(createTask("host1"), createTask("host2")));
        when(configuration.getElasticsearchNodes()).thenReturn(3);
        final OfferStrategy.OfferResult offerResult = offerStrategy.evaluate(baseOfferBuilder("host3")
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
        when(clusterState.getTaskList()).thenReturn(asList(createTask("host1"), createTask("host2")));
        when(configuration.getElasticsearchNodes()).thenReturn(3);
        final OfferStrategy.OfferResult offerResult = offerStrategy.evaluate(baseOfferBuilder("host3")
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
}