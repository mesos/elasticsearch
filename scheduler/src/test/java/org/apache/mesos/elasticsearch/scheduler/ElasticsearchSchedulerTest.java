package org.apache.mesos.elasticsearch.scheduler;

import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.elasticsearch.scheduler.matcher.RequestMatcher;
import org.apache.mesos.elasticsearch.scheduler.state.FrameworkState;
import org.apache.mesos.elasticsearch.scheduler.state.TestSerializableStateImpl;
import org.apache.mesos.elasticsearch.scheduler.util.ProtoTestUtil;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.InetSocketAddress;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.UUID;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.apache.mesos.elasticsearch.common.Offers.newOfferBuilder;
import static org.apache.mesos.elasticsearch.scheduler.Resources.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests Scheduler API.
 */
public class ElasticsearchSchedulerTest {

    private static final int LOCALHOST_IP = 2130706433;

    private static final Date TASK1_DATE = new DateTime()
            .withYear(2015)
            .withMonthOfYear(3)
            .withDayOfMonth(6)
            .withHourOfDay(10)
            .withMinuteOfHour(20)
            .withSecondOfMinute(40)
            .withMillisOfSecond(789)
            .toDate();

    private static final Date TASK2_DATE = new DateTime()
            .withYear(2015)
            .withMonthOfYear(3)
            .withDayOfMonth(6)
            .withHourOfDay(10)
            .withMinuteOfHour(20)
            .withSecondOfMinute(40)
            .withMillisOfSecond(900)
            .toDate();

    private ElasticsearchScheduler scheduler;

    private SchedulerDriver driver;

    private Protos.FrameworkID frameworkID;

    private Protos.MasterInfo masterInfo;

    private TaskInfoFactory taskInfoFactory;

    private org.apache.mesos.elasticsearch.scheduler.Configuration configuration;

    @Before
    public void before() {
        frameworkID = Protos.FrameworkID.newBuilder().setValue(UUID.randomUUID().toString()).build();
        FrameworkState frameworkState = mock(FrameworkState.class);
        when(frameworkState.getFrameworkID()).thenReturn(frameworkID);

        configuration = mock(org.apache.mesos.elasticsearch.scheduler.Configuration.class);
        when(configuration.getFrameworkState()).thenReturn(frameworkState);
        when(configuration.getFrameworkId()).thenReturn(frameworkID);
        when(configuration.getElasticsearchNodes()).thenReturn(3);
        when(configuration.getMesosZKURL()).thenReturn("zk://zookeeper:2181/mesos");
        when(configuration.getFrameworkZKURL()).thenReturn("zk://zookeeper:2181/mesos");
        when(configuration.getTaskName()).thenReturn("esdemo");
        when(configuration.getState()).thenReturn(new TestSerializableStateImpl());
        when(configuration.getExecutorHealthDelay()).thenReturn(10L);
        when(configuration.getExecutorTimeout()).thenReturn(10L);
        when(configuration.getFrameworkRole()).thenReturn("*");

        taskInfoFactory = mock(TaskInfoFactory.class);

        scheduler = new ElasticsearchScheduler(configuration, taskInfoFactory);

        driver = mock(SchedulerDriver.class);

        masterInfo = newMasterInfo();
        scheduler.registered(driver, frameworkID, masterInfo);
        scheduler.offerStrategy = mock(OfferStrategy.class);
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

        scheduler.resourceOffers(driver, singletonList(offer));

        verify(driver).declineOffer(offer.getId());
    }

    @Test
    public void testResourceOffers_launchTasks() {
        final Protos.Offer offer = newOffer("host3").build();
        when(scheduler.offerStrategy.evaluate(offer)).thenReturn(OfferStrategy.OfferResult.accept());

        Protos.TaskInfo taskInfo = ProtoTestUtil.getDefaultTaskInfo();

        when(taskInfoFactory.createTask(configuration, offer)).thenReturn(taskInfo);

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
