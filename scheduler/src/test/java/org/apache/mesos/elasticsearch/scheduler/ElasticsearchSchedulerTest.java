package org.apache.mesos.elasticsearch.scheduler;

import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.elasticsearch.scheduler.matcher.RequestMatcher;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.ZonedDateTime;
import java.util.*;

import static java.util.Collections.*;
import static org.apache.mesos.elasticsearch.common.Offers.newOfferBuilder;
import static org.apache.mesos.elasticsearch.scheduler.Resources.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.spockframework.util.CollectionUtil.asSet;

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
    private ZonedDateTime now = ZonedDateTime.now();


    @Before
    public void before() {
        Clock clock = mock(Clock.class);
        when(clock.now()).thenReturn(TASK1_DATE).thenReturn(TASK2_DATE);

        frameworkID = Protos.FrameworkID.newBuilder().setValue(UUID.randomUUID().toString()).build();

        configuration = mock(org.apache.mesos.elasticsearch.scheduler.Configuration.class);
        when(configuration.getFrameworkId()).thenReturn(frameworkID);
        when(configuration.getNumberOfHwNodes()).thenReturn(3);
        when(configuration.getZookeeperHost()).thenReturn("zookeeper");
        when(configuration.getZookeeperPort()).thenReturn(2181);
        when(configuration.getTaskName()).thenReturn("esdemo");

        taskInfoFactory = mock(TaskInfoFactory.class);

        scheduler = new ElasticsearchScheduler(configuration, taskInfoFactory);

        driver = mock(SchedulerDriver.class);

        masterInfo = newMasterInfo();
    }

    @Test
    public void testRegistered() {
        scheduler.registered(driver, frameworkID, masterInfo);

        Mockito.verify(driver).requestResources(Mockito.argThat(new RequestMatcher().cpus(Configuration.CPUS).mem(Configuration.MEM).disk(Configuration.DISK)));
    }

    @Test
    public void testResourceOffers_isSlaveAlreadyRunningTask() {
        scheduler.tasks = asSet(new Task[]{new Task("host1", "1", now), new Task("host2", "2", now)});

        Protos.Offer.Builder offer = newOffer("host1");

        scheduler.resourceOffers(driver, singletonList(offer.build()));

        verify(driver).declineOffer(offer.getId());
    }

    @Test
    public void testResourceOffers_enoughNodes() {
        scheduler.tasks = asSet(new Task[]{new Task("host1", "1", now), new Task("host2", "2", now), new Task("host3", "3", now)});

        Protos.Offer.Builder offer = newOffer("host4");

        scheduler.resourceOffers(driver, singletonList(offer.build()));

        verify(driver).declineOffer(offer.getId());
    }

    @Test
    public void testResourceOffers_noPorts() {
        scheduler.tasks = asSet(new Task[]{new Task("host1", "1", now), new Task("host2", "2", now)});

        Protos.Offer.Builder offer = newOffer("host3");

        scheduler.resourceOffers(driver, singletonList(offer.build()));

        verify(driver).declineOffer(offer.getId());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testResourceOffers_singlePort() {
        scheduler.tasks = asSet(new Task[]{new Task("host1", "task1", now)});

        Protos.Offer.Builder offerBuilder = newOffer("host3");
        offerBuilder.addResources(portRange(9200, 9200));

        scheduler.resourceOffers(driver, singletonList(offerBuilder.build()));

        Mockito.verify(driver).declineOffer(offerBuilder.build().getId());
    }

    @Test
    public void testResourceOffers_launchTasks() {
        scheduler.tasks = new HashSet<>();

        Protos.Offer.Builder offerBuilder = newOffer("host3");
        offerBuilder.addResources(portRange(9200, 9200));
        offerBuilder.addResources(portRange(9300, 9300));

        Protos.TaskInfo taskInfo = Protos.TaskInfo.newBuilder()
                                        .setName(configuration.getTaskName())
                                        .setTaskId(Protos.TaskID.newBuilder().setValue(UUID.randomUUID().toString()))
                                        .setSlaveId(Protos.SlaveID.newBuilder().setValue(UUID.randomUUID().toString()).build())
                                        .build();

        when(taskInfoFactory.createTask(offerBuilder.build(), frameworkID, configuration)).thenReturn(taskInfo);

        scheduler.resourceOffers(driver, singletonList(offerBuilder.build()));

        verify(driver).launchTasks(Collections.singleton(offerBuilder.build().getId()), Collections.singleton(taskInfo));
    }

    private Protos.Offer.Builder newOffer(String hostname) {
        Protos.Offer.Builder builder = newOfferBuilder(UUID.randomUUID().toString(), hostname, UUID.randomUUID().toString(), frameworkID);
        builder.addResources(cpus(Configuration.CPUS));
        builder.addResources(mem(Configuration.MEM));
        builder.addResources(disk(Configuration.DISK));
        return builder;
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
