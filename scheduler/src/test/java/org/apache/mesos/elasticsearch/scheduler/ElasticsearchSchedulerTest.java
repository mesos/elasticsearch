package org.apache.mesos.elasticsearch.scheduler;

import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.elasticsearch.scheduler.matcher.RequestMatcher;
import org.apache.mesos.elasticsearch.scheduler.state.ClusterState;
import org.apache.mesos.elasticsearch.scheduler.state.FrameworkState;
import org.apache.mesos.elasticsearch.scheduler.state.SerializableState;
import org.apache.mesos.elasticsearch.scheduler.util.ProtoTestUtil;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

import java.util.Observer;
import java.util.UUID;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.apache.mesos.elasticsearch.common.Offers.newOfferBuilder;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests Scheduler API.
 */
public class ElasticsearchSchedulerTest {

    private static final int LOCALHOST_IP = 2130706433;

    private ElasticsearchScheduler scheduler;

    private SchedulerDriver driver;

    private Protos.FrameworkID frameworkID;

    private Protos.MasterInfo masterInfo;

    private TaskInfoFactory taskInfoFactory;

    private FrameworkState frameworkState = mock(FrameworkState.class);
    private ClusterState clusterState = mock(ClusterState.class);

    private org.apache.mesos.elasticsearch.scheduler.Configuration configuration;
    private SerializableState serializableState = mock(SerializableState.class);

    @Before
    public void before() {
        frameworkID = Protos.FrameworkID.newBuilder().setValue(UUID.randomUUID().toString()).build();
        when(frameworkState.getFrameworkID()).thenReturn(frameworkID);

        configuration = mock(org.apache.mesos.elasticsearch.scheduler.Configuration.class);
        when(configuration.getElasticsearchNodes()).thenReturn(3);
        when(configuration.getMesosZKURL()).thenReturn("zk://zookeeper:2181/mesos");
        when(configuration.getFrameworkZKURL()).thenReturn("zk://zookeeper:2181/mesos");
        when(configuration.getTaskName()).thenReturn("esdemo");
        when(configuration.getExecutorHealthDelay()).thenReturn(10L);
        when(configuration.getExecutorTimeout()).thenReturn(10L);
        when(configuration.getFrameworkRole()).thenReturn("*");

        taskInfoFactory = mock(TaskInfoFactory.class);

        scheduler = new ElasticsearchScheduler(configuration, frameworkState, clusterState, taskInfoFactory, serializableState);

        driver = mock(SchedulerDriver.class);

        masterInfo = newMasterInfo();
        scheduler.registered(driver, frameworkID, masterInfo);
        scheduler.offerStrategy = mock(OfferStrategy.class);
    }

    @Test
    public void shouldCallObserversWhenExecutorLost() {
        Protos.ExecutorID executorID = ProtoTestUtil.getExecutorId();
        Protos.SlaveID slaveID = ProtoTestUtil.getSlaveId();

        when(clusterState.getTask(executorID)).thenReturn(ProtoTestUtil.getDefaultTaskInfo());
        final Observer observer = mock(Observer.class);
        scheduler.addObserver(observer);
        scheduler.executorLost(driver, executorID, slaveID, 1);

        verify(observer).update(eq(scheduler), argThat(new ArgumentMatcher<Protos.TaskStatus>() {
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

        when(scheduler.offerStrategy.evaluate(offer)).thenReturn(OfferStrategy.OfferResult.decline("Test"));
        when(frameworkState.isRegistered()).thenReturn(true);

        scheduler.resourceOffers(driver, singletonList(offer));

        verify(driver).declineOffer(offer.getId());
    }

    @Test
    public void testResourceOffers_launchTasks() {
        final Protos.Offer offer = newOffer("host3").build();
        when(scheduler.offerStrategy.evaluate(offer)).thenReturn(OfferStrategy.OfferResult.accept());
        when(frameworkState.isRegistered()).thenReturn(true);

        Protos.TaskInfo taskInfo = ProtoTestUtil.getDefaultTaskInfo();

        when(taskInfoFactory.createTask(configuration, frameworkState, offer)).thenReturn(taskInfo);

        scheduler.resourceOffers(driver, singletonList(offer));

        verify(driver).launchTasks(singleton(offer.getId()), singleton(taskInfo));
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
