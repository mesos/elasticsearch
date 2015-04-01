package org.apache.mesos.elasticsearch.scheduler;

import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.elasticsearch.scheduler.matcher.OfferIDMatcher;
import org.apache.mesos.elasticsearch.scheduler.matcher.RequestMatcher;
import org.apache.mesos.elasticsearch.scheduler.matcher.TaskInfoMatcher;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

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

    @Before
    public void before() {
        Clock clock = Mockito.mock(Clock.class);
        Mockito.when(clock.now()).thenReturn(TASK1_DATE).thenReturn(TASK2_DATE);

        scheduler = new ElasticsearchScheduler("http://master:5050", 3, false, "master:8020");
        scheduler.clock = clock;

        driver = Mockito.mock(SchedulerDriver.class);
        frameworkID = newFrameworkId();
        masterInfo = newMasterInfo();
    }

    @Test
    public void testRegistered() {
        scheduler.registered(driver, frameworkID, masterInfo);

        Mockito.verify(driver).requestResources(Mockito.argThat(new RequestMatcher().cpus(1.0)));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testResourceOffers_singleOffer() {
        Protos.Offer offer = newOffer("offer1", "host1", "slave1");

        scheduler.resourceOffers(driver, Collections.singletonList(offer));

        OfferIDMatcher offerIdMatcher = new OfferIDMatcher("offer1");
        TaskInfoMatcher taskInfoMatcher = new TaskInfoMatcher("elasticsearch_host1_20150306T102040.789Z").slaveId("slave1").cpus(1.0).mem(2048).disk(1000d);

        Mockito.verify(driver).launchTasks((Collection<Protos.OfferID>) Matchers.argThat(org.hamcrest.Matchers.contains(offerIdMatcher)),
                (Collection<Protos.TaskInfo>) Matchers.argThat(org.hamcrest.Matchers.contains(taskInfoMatcher)));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testResourceOffers_twoOffers() {
        Protos.Offer offer1 = newOffer("offer1", "host1", "slave1");
        Protos.Offer offer2 = newOffer("offer2", "host2", "slave2");

        scheduler.resourceOffers(driver, Arrays.asList(offer1, offer2));

        OfferIDMatcher offerIdMatcher1 = new OfferIDMatcher("offer1");
        OfferIDMatcher offerIdMatcher2 = new OfferIDMatcher("offer2");

        TaskInfoMatcher taskInfoMatcher1 = new TaskInfoMatcher("elasticsearch_host1_20150306T102040.789Z").slaveId("slave1").cpus(1.0).mem(2048).disk(1000d);
        TaskInfoMatcher taskInfoMatcher2 = new TaskInfoMatcher("elasticsearch_host2_20150306T102040.900Z").slaveId("slave2").cpus(1.0).mem(2048).disk(1000d);

        Mockito.verify(driver).launchTasks((Collection<Protos.OfferID>) Mockito.argThat(org.hamcrest.Matchers.contains(offerIdMatcher1)), (Collection<Protos.TaskInfo>) Mockito.argThat(org.hamcrest.Matchers.contains(taskInfoMatcher1)));
        Mockito.verify(driver).launchTasks((Collection<Protos.OfferID>) Mockito.argThat(org.hamcrest.Matchers.contains(offerIdMatcher2)), (Collection<Protos.TaskInfo>) Mockito.argThat(org.hamcrest.Matchers.contains(taskInfoMatcher2)));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testResourceOffers_haveEnoughNodes() {
        Set<Task> tasks = new HashSet<>();
        tasks.add(new Task("host1", "task1"));
        tasks.add(new Task("host2", "task2"));
        scheduler.tasks = tasks;

        Protos.Offer offer1 = newOffer("offer1", "host3", "slave1");
        Protos.Offer offer2 = newOffer("offer2", "host3", "slave2");

        scheduler.resourceOffers(driver, Arrays.asList(offer1, offer2));

        OfferIDMatcher offerIdMatcher1 = new OfferIDMatcher("offer1");
        TaskInfoMatcher taskInfoMatcher1 = new TaskInfoMatcher("elasticsearch_host3_20150306T102040.789Z").slaveId("slave1").cpus(1.0).mem(2048).disk(1000d);

        Mockito.verify(driver).launchTasks((Collection<Protos.OfferID>) Mockito.argThat(org.hamcrest.Matchers.contains(offerIdMatcher1)), (Collection<Protos.TaskInfo>) Mockito.argThat(org.hamcrest.Matchers.contains(taskInfoMatcher1)));
        Mockito.verify(driver).declineOffer(Protos.OfferID.newBuilder().setValue("offer2").build());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testResourceOffers_isOfferGood() {
        Set<Task> tasks = new HashSet<>();
        tasks.add(new Task("host1", "task1"));
        scheduler.tasks = tasks;

        Protos.Offer offer1 = newOffer("offer1", "host1", "slave1");

        scheduler.resourceOffers(driver, Collections.singletonList(offer1));

        Mockito.verify(driver).declineOffer(Protos.OfferID.newBuilder().setValue("offer1").build());
    }

    private String uuid() {
        return "" + UUID.randomUUID();
    }

    private Protos.Offer newOffer(String offerId, String hostname, String slave) {
        Protos.OfferID offerID = Protos.OfferID.newBuilder().setValue(offerId).build();
        Protos.SlaveID slaveID = Protos.SlaveID.newBuilder().setValue(slave).build();
        return Protos.Offer.newBuilder().setId(offerID).setFrameworkId(frameworkID).setSlaveId(slaveID).setHostname(hostname).build();
    }

    private Protos.FrameworkID newFrameworkId() {
        return Protos.FrameworkID.newBuilder().setValue(uuid()).build();
    }

    private Protos.MasterInfo newMasterInfo() {
        return Protos.MasterInfo.newBuilder()
                .setId(uuid())
                .setIp(LOCALHOST_IP)
                .setPort(5050)
                .setHostname("master")
                .build();
    }

}
