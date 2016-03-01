package org.apache.mesos.elasticsearch.scheduler;

import org.apache.log4j.Logger;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.elasticsearch.scheduler.matcher.RequestMatcher;
import org.apache.mesos.elasticsearch.scheduler.state.ClusterState;
import org.apache.mesos.elasticsearch.scheduler.state.FrameworkState;
import org.apache.mesos.elasticsearch.scheduler.state.SerializableState;
import org.apache.mesos.elasticsearch.scheduler.state.StatePath;
import org.apache.mesos.elasticsearch.scheduler.util.ProtoTestUtil;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.UUID;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.apache.mesos.elasticsearch.common.Offers.newOfferBuilder;
import static org.mockito.Mockito.*;

/**
 * Tests Scheduler API.
 */
public class ElasticsearchSchedulerTest {
    private static final Logger LOGGER = Logger.getLogger(ElasticsearchSchedulerTest.class);

    private static final int LOCALHOST_IP = 2130706433;

    private ElasticsearchScheduler scheduler;

    private SchedulerDriver driver = mock(SchedulerDriver.class);

    private Protos.FrameworkID frameworkID = Protos.FrameworkID.newBuilder().setValue(UUID.randomUUID().toString()).build();

    private Protos.MasterInfo masterInfo;

    private TaskInfoFactory taskInfoFactory;

    private StatePath statePath = mock(StatePath.class);

    private FrameworkState frameworkState = mock(FrameworkState.class);
    private ClusterState clusterState = mock(ClusterState.class);

    private org.apache.mesos.elasticsearch.scheduler.Configuration configuration;
    private SerializableState serializableState = mock(SerializableState.class);
    private OfferStrategyNormal offerStrategy = mock(OfferStrategyNormal.class);

    @Before
    public void before() {
        when(frameworkState.getFrameworkID()).thenReturn(frameworkID);

        configuration = mock(org.apache.mesos.elasticsearch.scheduler.Configuration.class);
        when(configuration.getElasticsearchNodes()).thenReturn(3);
        when(configuration.getMesosZKURL()).thenReturn("zk://zookeeper:2181/mesos");
        when(configuration.getTaskName()).thenReturn("esdemo");
        when(configuration.getFrameworkRole()).thenReturn("*");
        when(configuration.getFrameworkName()).thenReturn("FrameworkName");

        taskInfoFactory = mock(TaskInfoFactory.class);


        scheduler = new ElasticsearchScheduler(configuration, frameworkState, clusterState, taskInfoFactory, offerStrategy, serializableState, statePath);

        masterInfo = newMasterInfo();
        scheduler.registered(driver, frameworkID, masterInfo);
    }

    @Test
    public void willRunDriver() throws Exception {
        scheduler.run(driver);
        verify(driver).run();
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

        when(taskInfoFactory.createTask(any(), any(), any(), any())).thenReturn(taskInfo);

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
