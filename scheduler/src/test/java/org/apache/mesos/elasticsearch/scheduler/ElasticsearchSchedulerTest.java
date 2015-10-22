package org.apache.mesos.elasticsearch.scheduler;

import org.apache.log4j.Logger;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.elasticsearch.common.AdaptorIPAddress;
import org.apache.mesos.elasticsearch.common.SerializableIPAddress;
import org.apache.mesos.elasticsearch.scheduler.matcher.RequestMatcher;
import org.apache.mesos.elasticsearch.scheduler.state.ClusterState;
import org.apache.mesos.elasticsearch.scheduler.state.FrameworkState;
import org.apache.mesos.elasticsearch.scheduler.state.SerializableState;
import org.apache.mesos.elasticsearch.scheduler.util.ProtoTestUtil;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

import java.net.SocketException;
import java.util.UUID;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.apache.mesos.elasticsearch.common.Offers.newOfferBuilder;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.*;

/**
 * Tests Scheduler API.
 */
public class ElasticsearchSchedulerTest {
    private static final Logger LOGGER = Logger.getLogger(ElasticsearchSchedulerTest.class);

    private static final int LOCALHOST_IP = 2130706433;

    private ElasticsearchScheduler scheduler;

    private SchedulerDriver driver;

    private Protos.FrameworkID frameworkID = Protos.FrameworkID.newBuilder().setValue(UUID.randomUUID().toString()).build();

    private Protos.MasterInfo masterInfo;

    private TaskInfoFactory taskInfoFactory;

    private FrameworkState frameworkState = mock(FrameworkState.class);
    private ClusterState clusterState = mock(ClusterState.class);

    private org.apache.mesos.elasticsearch.scheduler.Configuration configuration;
    private SerializableState serializableState = mock(SerializableState.class);
    private OfferStrategy offerStrategy = mock(OfferStrategy.class);

    @Before
    public void before() {
        when(frameworkState.getFrameworkID()).thenReturn(frameworkID);

        configuration = mock(org.apache.mesos.elasticsearch.scheduler.Configuration.class);
        when(configuration.getElasticsearchNodes()).thenReturn(3);
        when(configuration.getMesosZKURL()).thenReturn("zk://zookeeper:2181/mesos");
        when(configuration.getFrameworkZKURL()).thenReturn("zk://zookeeper:2181/mesos");
        when(configuration.getTaskName()).thenReturn("esdemo");
        when(configuration.getExecutorHealthDelay()).thenReturn(10L);
        when(configuration.getExecutorTimeout()).thenReturn(10L);
        when(configuration.getFrameworkRole()).thenReturn("*");
        when(configuration.getFrameworkName()).thenReturn("FrameworkName");

        taskInfoFactory = mock(TaskInfoFactory.class);

        scheduler = new ElasticsearchScheduler(configuration, frameworkState, clusterState, taskInfoFactory, offerStrategy, serializableState);

        driver = mock(SchedulerDriver.class);

        masterInfo = newMasterInfo();
        scheduler.registered(driver, frameworkID, masterInfo);
    }

    @Test
    public void shouldCallObserversWhenExecutorLost() {
        Protos.ExecutorID executorID = ProtoTestUtil.getExecutorId();
        Protos.SlaveID slaveID = ProtoTestUtil.getSlaveId();

        when(clusterState.getTask(executorID)).thenReturn(ProtoTestUtil.getDefaultTaskInfo());
        scheduler.executorLost(driver, executorID, slaveID, 1);

        verify(frameworkState).announceStatusUpdate(argThat(new ArgumentMatcher<Protos.TaskStatus>() {
            @Override
            public boolean matches(Object argument) {
                return ((Protos.TaskStatus) argument).getState().equals(Protos.TaskState.TASK_LOST);
            }
        }));
    }

    @Test
    public void testRegistered() {
        verify(driver).requestResources(
                Mockito.argThat(
                        new RequestMatcher(
                                configuration.getCpus(),
                                configuration.getMem(),
                                configuration.getDisk(),
                                configuration.getFrameworkRole()
                        )
                )
        );
    }

    @Test
    public void willDeclineOfferIfStrategyDeclinesOffer() {
        Protos.Offer offer = newOffer("host1").build();

        when(offerStrategy.evaluate(offer)).thenReturn(OfferStrategy.OfferResult.decline("Test"));
        when(frameworkState.isRegistered()).thenReturn(true);

        scheduler.resourceOffers(driver, singletonList(offer));

        verify(driver).declineOffer(offer.getId());
    }

    @Test
    public void testResourceOffers_launchTasks() {
        final Protos.Offer offer = newOffer("host3").build();
        when(offerStrategy.evaluate(offer)).thenReturn(OfferStrategy.OfferResult.accept());
        when(frameworkState.isRegistered()).thenReturn(true);

        Protos.TaskInfo taskInfo = ProtoTestUtil.getDefaultTaskInfo();

        when(taskInfoFactory.createTask(configuration, frameworkState, offer)).thenReturn(taskInfo);

        scheduler.resourceOffers(driver, singletonList(offer));

        verify(driver).launchTasks(singleton(offer.getId()), singleton(taskInfo));
    }

    @Test
    public void shouldRunWithCredentials() {
        when(configuration.getFrameworkPrincipal()).thenReturn("user1");
        when(configuration.getFrameworkSecretPath()).thenReturn("/etc/passwd");
        try {
            scheduler.run();
        } catch (java.lang.UnsatisfiedLinkError e) {
            LOGGER.info("This error is normal. Don't worry.");
        }
        verify(configuration, atLeastOnce()).getFrameworkPrincipal();
        verify(configuration, atLeastOnce()).getFrameworkSecretPath();
    }

    @Test
    public void shouldUpdateTaskInfoWhenIPAddressReceived() throws SocketException {
        when(clusterState.getTask(ProtoTestUtil.getExecutorId())).thenReturn(ProtoTestUtil.getDefaultTaskInfo());
        when(taskInfoFactory.toData(anyString(), anyString(), any())).thenCallRealMethod();
        scheduler.frameworkMessage(driver, ProtoTestUtil.getExecutorId(), ProtoTestUtil.getSlaveId(), new SerializableIPAddress(AdaptorIPAddress.eth0()).toBytes());
        verify(clusterState, times(1)).removeTask(ProtoTestUtil.getDefaultTaskInfo());
        verify(clusterState, times(1)).addTask(any(Protos.TaskInfo.class));
    }

    private Protos.Offer.Builder newOffer(String hostname) {
        return newOfferBuilder(UUID.randomUUID().toString(), hostname, UUID.randomUUID().toString(), frameworkID);
    }

    private Protos.MasterInfo newMasterInfo() {
        return Protos.MasterInfo.newBuilder()
                .setId(UUID.randomUUID().toString())
                .setIp(LOCALHOST_IP)
                .setPort(5050)
                .setHostname("master")
                .build();
    }

}
