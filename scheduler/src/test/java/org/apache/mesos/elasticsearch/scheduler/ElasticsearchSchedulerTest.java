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
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.UUID;

import static java.util.Collections.singletonList;
import static org.apache.mesos.elasticsearch.common.Offers.newOfferBuilder;
import static org.apache.mesos.elasticsearch.scheduler.Resources.*;
import static org.mockito.Mockito.*;

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
    private InetSocketAddress transportAddress = new InetSocketAddress("localhost", 9300);
    private InetSocketAddress clientAddress = new InetSocketAddress("localhost", 9200);


    @Before
    public void before() {
        Clock clock = mock(Clock.class);
        when(clock.now()).thenReturn(TASK1_DATE).thenReturn(TASK2_DATE);

        frameworkID = Protos.FrameworkID.newBuilder().setValue(UUID.randomUUID().toString()).build();
        FrameworkState frameworkState = mock(FrameworkState.class);
        when(frameworkState.getFrameworkID()).thenReturn(frameworkID);

        configuration = mock(org.apache.mesos.elasticsearch.scheduler.Configuration.class);
        when(configuration.getFrameworkState()).thenReturn(frameworkState);
        when(configuration.getFrameworkId()).thenReturn(frameworkID);
        when(configuration.getNumberOfElasticsearchNodes()).thenReturn(3);
        when(configuration.getMesosZKURL()).thenReturn("zk://zookeeper:2181/mesos");
        when(configuration.getTaskName()).thenReturn("esdemo");
        when(configuration.getState()).thenReturn(new TestSerializableStateImpl());

        taskInfoFactory = mock(TaskInfoFactory.class);

        scheduler = new ElasticsearchScheduler(configuration, taskInfoFactory);

        driver = mock(SchedulerDriver.class);

        masterInfo = newMasterInfo();
        scheduler.registered(driver, frameworkID, masterInfo);
    }

    @Test
    public void testRegistered() {
        Mockito.verify(driver).requestResources(Mockito.argThat(new RequestMatcher().cpus(configuration.getCpus()).mem(configuration.getMem()).disk(configuration.getDisk())));
    }

    @Test
    public void testResourceOffers_isSlaveAlreadyRunningTask() {
        Task task1 = new Task("host1", "1", Protos.TaskState.TASK_RUNNING, now, clientAddress, transportAddress);
        Task task2 = new Task("host2", "2", Protos.TaskState.TASK_RUNNING, now, clientAddress, transportAddress);
        scheduler.tasks = new HashMap<>();
        scheduler.tasks.put("host1", task1);
        scheduler.tasks.put("host2", task2);

        Protos.Offer.Builder offer = newOffer("host1");

        scheduler.resourceOffers(driver, singletonList(offer.build()));

        verify(driver).declineOffer(offer.getId());
    }

    @Test
    public void testResourceOffers_enoughNodes() {
        Task task1 = new Task("host1", "1", Protos.TaskState.TASK_RUNNING, now, clientAddress, transportAddress);
        Task task2 = new Task("host2", "2", Protos.TaskState.TASK_RUNNING, now, clientAddress, transportAddress);
        Task task3 = new Task("host3", "3", Protos.TaskState.TASK_RUNNING, now, clientAddress, transportAddress);
        scheduler.tasks = new HashMap<>();
        scheduler.tasks.put("host1", task1);
        scheduler.tasks.put("host2", task2);
        scheduler.tasks.put("host3", task3);

        Protos.Offer.Builder offer = newOffer("host4");

        scheduler.resourceOffers(driver, singletonList(offer.build()));

        verify(driver).declineOffer(offer.getId());
    }

    @Test
    public void testResourceOffers_noPorts() {
        Task task1 = new Task("host1", "1", Protos.TaskState.TASK_RUNNING, now, clientAddress, transportAddress);
        Task task2 = new Task("host2", "2", Protos.TaskState.TASK_RUNNING, now, clientAddress, transportAddress);
        scheduler.tasks = new HashMap<>();
        scheduler.tasks.put("host1", task1);
        scheduler.tasks.put("host2", task2);

        Protos.Offer.Builder offer = newOffer("host3");

        scheduler.resourceOffers(driver, singletonList(offer.build()));

        verify(driver).declineOffer(offer.getId());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testResourceOffers_singlePort() {
        Task task = new Task("host1", "task1", Protos.TaskState.TASK_RUNNING, now, clientAddress, transportAddress);
        scheduler.tasks = new HashMap<>();
        scheduler.tasks.put("host1", task);

        Protos.Offer.Builder offerBuilder = newOffer("host3");
        offerBuilder.addResources(portRange(9200, 9200));

        scheduler.resourceOffers(driver, singletonList(offerBuilder.build()));

        Mockito.verify(driver).declineOffer(offerBuilder.build().getId());
    }

    @Test
    public void testResourceOffers_launchTasks() {
        scheduler.tasks = new HashMap<>();

        Protos.Offer.Builder offerBuilder = newOffer("host3");
        offerBuilder.addResources(portRange(9200, 9200));
        offerBuilder.addResources(portRange(9300, 9300));

        Protos.TaskInfo taskInfo = ProtoTestUtil.getDefaultTaskInfo();

        when(taskInfoFactory.createTask(configuration, offerBuilder.build())).thenReturn(taskInfo);

        scheduler.resourceOffers(driver, singletonList(offerBuilder.build()));

        verify(driver).launchTasks(Collections.singleton(offerBuilder.build().getId()), Collections.singleton(taskInfo));
    }

    private Protos.Offer.Builder newOffer(String hostname) {
        Protos.Offer.Builder builder = newOfferBuilder(UUID.randomUUID().toString(), hostname, UUID.randomUUID().toString(), frameworkID);
        builder.addResources(cpus(configuration.getCpus()));
        builder.addResources(mem(configuration.getMem()));
        builder.addResources(disk(configuration.getDisk()));
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
