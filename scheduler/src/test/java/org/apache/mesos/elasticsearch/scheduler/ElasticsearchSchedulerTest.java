package org.apache.mesos.elasticsearch.scheduler;

import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.elasticsearch.common.Configuration;
import org.apache.mesos.elasticsearch.scheduler.matcher.OfferIDMatcher;
import org.apache.mesos.elasticsearch.scheduler.matcher.RequestMatcher;
import org.apache.mesos.elasticsearch.scheduler.matcher.TaskInfoMatcher;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.util.*;

import static org.apache.mesos.elasticsearch.common.Offers.newOfferBuilder;
import static org.apache.mesos.elasticsearch.common.Resources.*;

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
    private State state;

    private Protos.FrameworkID frameworkID;
    private Protos.MasterInfo masterInfo;

    @Before
    public void before() {
        Clock clock = Mockito.mock(Clock.class);
        Mockito.when(clock.now()).thenReturn(TASK1_DATE).thenReturn(TASK2_DATE);

        state = Mockito.mock(State.class);
        scheduler = new ElasticsearchScheduler("http://master:5050", "dns", 3, false, "master:8020", state);
        scheduler.clock = clock;

        driver = Mockito.mock(SchedulerDriver.class);
        frameworkID = Protos.FrameworkID.newBuilder().setValue(UUID.randomUUID().toString()).build();
        masterInfo = newMasterInfo();
    }

    @Test
    public void testRegistered() {
        scheduler.registered(driver, frameworkID, masterInfo);

        Mockito.verify(driver).requestResources(Mockito.argThat(new RequestMatcher().cpus(Configuration.CPUS).mem(Configuration.MEM).disk(Configuration.DISK)));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testResourceOffers_singleOffer() {
        Protos.Offer.Builder offerBuilder = newOfferBuilder("offer1", "host1", "slave1", frameworkID);
        offerBuilder.addResources(cpus(Configuration.CPUS));
        offerBuilder.addResources(mem(Configuration.MEM));
        offerBuilder.addResources(disk(Configuration.DISK));
        offerBuilder.addResources(portRange(Configuration.BEGIN_PORT, Configuration.END_PORT));

        scheduler.registered(driver, frameworkID, masterInfo);
        scheduler.resourceOffers(driver, Collections.singletonList(offerBuilder.build()));

        OfferIDMatcher offerIdMatcher = new OfferIDMatcher("offer1");

        TaskInfoMatcher taskInfoMatcher = new TaskInfoMatcher("elasticsearch_host1_20150306T102040.789Z").slaveId("slave1").cpus(Configuration.CPUS).mem(Configuration.MEM).disk(Configuration.DISK);

        Mockito.verify(driver).launchTasks((Collection<Protos.OfferID>) Matchers.argThat(org.hamcrest.Matchers.contains(offerIdMatcher)),
                (Collection<Protos.TaskInfo>) Matchers.argThat(org.hamcrest.Matchers.contains(taskInfoMatcher)));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testResourceOffers_twoOffers() {
        Protos.Offer.Builder offerBuilder1 = newOfferBuilder("offer1", "host1", "slave1", frameworkID);
        offerBuilder1.addResources(cpus(Configuration.CPUS));
        offerBuilder1.addResources(mem(Configuration.MEM));
        offerBuilder1.addResources(disk(Configuration.DISK));
        offerBuilder1.addResources(portRange(Configuration.BEGIN_PORT, Configuration.END_PORT));

        Protos.Offer.Builder offerBuilder2 = newOfferBuilder("offer2", "host2", "slave2", frameworkID);
        offerBuilder2.addResources(cpus(Configuration.CPUS));
        offerBuilder2.addResources(mem(Configuration.MEM));
        offerBuilder2.addResources(disk(Configuration.DISK));
        offerBuilder2.addResources(portRange(Configuration.BEGIN_PORT, Configuration.END_PORT));

        scheduler.registered(driver, frameworkID, masterInfo);
        scheduler.resourceOffers(driver, Arrays.asList(offerBuilder1.build(), offerBuilder2.build()));

        OfferIDMatcher offerIdMatcher1 = new OfferIDMatcher("offer1");
        OfferIDMatcher offerIdMatcher2 = new OfferIDMatcher("offer2");

        TaskInfoMatcher taskInfoMatcher1 = new TaskInfoMatcher("elasticsearch_host1_20150306T102040.789Z").slaveId("slave1").cpus(Configuration.CPUS).mem(Configuration.MEM).disk(Configuration.DISK);
        TaskInfoMatcher taskInfoMatcher2 = new TaskInfoMatcher("elasticsearch_host2_20150306T102040.900Z").slaveId("slave2").cpus(Configuration.CPUS).mem(Configuration.MEM).disk(Configuration.DISK);

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

        Protos.Offer.Builder offerBuilder1 = newOfferBuilder("offer1", "host3", "slave1", frameworkID);
        offerBuilder1.addResources(cpus(Configuration.CPUS));
        offerBuilder1.addResources(mem(Configuration.MEM));
        offerBuilder1.addResources(disk(Configuration.DISK));
        offerBuilder1.addResources(portRange(Configuration.BEGIN_PORT, Configuration.END_PORT));

        Protos.Offer.Builder offerBuilder2 = newOfferBuilder("offer2", "host3", "slave2", frameworkID);
        offerBuilder2.addResources(cpus(Configuration.CPUS));
        offerBuilder2.addResources(mem(Configuration.MEM));
        offerBuilder2.addResources(disk(Configuration.DISK));
        offerBuilder2.addResources(portRange(Configuration.BEGIN_PORT, Configuration.END_PORT));

        scheduler.registered(driver, frameworkID, masterInfo);
        scheduler.resourceOffers(driver, Arrays.asList(offerBuilder1.build(), offerBuilder2.build()));

        OfferIDMatcher offerIdMatcher1 = new OfferIDMatcher("offer1");
        TaskInfoMatcher taskInfoMatcher1 = new TaskInfoMatcher("elasticsearch_host3_20150306T102040.789Z").slaveId("slave1").cpus(Configuration.CPUS).mem(Configuration.MEM).disk(Configuration.DISK);

        Mockito.verify(driver).launchTasks((Collection<Protos.OfferID>) Mockito.argThat(org.hamcrest.Matchers.contains(offerIdMatcher1)), (Collection<Protos.TaskInfo>) Mockito.argThat(org.hamcrest.Matchers.contains(taskInfoMatcher1)));
        Mockito.verify(driver).declineOffer(Protos.OfferID.newBuilder().setValue("offer2").build());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testResourceOffers_whenOfferedNoPortsReject() {
        Set<Task> tasks = new HashSet<>();
        tasks.add(new Task("host1", "task1"));
        scheduler.tasks = tasks;

        Protos.Offer.Builder offerBuilder = newOfferBuilder("offer1", "host1", "slave1", frameworkID);
        offerBuilder.addResources(cpus(Configuration.CPUS));
        offerBuilder.addResources(mem(Configuration.MEM));
        offerBuilder.addResources(disk(Configuration.DISK));
        offerBuilder.addResources(portRange(Configuration.BEGIN_PORT, Configuration.END_PORT));

        scheduler.resourceOffers(driver, Collections.singletonList(offerBuilder.build()));

        Mockito.verify(driver).declineOffer(Protos.OfferID.newBuilder().setValue("offer1").build());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testResourceOffers_whenOfferedSinglePortReject() {
        Set<Task> tasks = new HashSet<>();
        tasks.add(new Task("host1", "task1"));
        scheduler.tasks = tasks;

        Protos.Offer.Builder offerBuilder = newOfferBuilder("offer1", "host1", "slave1", frameworkID);
        offerBuilder.addResources(cpus(Configuration.CPUS));
        offerBuilder.addResources(mem(Configuration.MEM));
        offerBuilder.addResources(disk(Configuration.DISK));
        offerBuilder.addResources(portRange(Configuration.BEGIN_PORT, Configuration.END_PORT));
        offerBuilder.addResources(singlePortRange(9200));

        scheduler.resourceOffers(driver, Collections.singletonList(offerBuilder.build()));

        Mockito.verify(driver).declineOffer(Protos.OfferID.newBuilder().setValue("offer1").build());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testResourceOffers_whenOfferedTwoPortsAccept() {
        scheduler.tasks = new HashSet<>();

        scheduler.registered(driver, frameworkID, masterInfo);

        Protos.Offer.Builder offerBuilder = newOfferBuilder("offer1", "host1", "slave1", frameworkID);
        offerBuilder.addResources(cpus(Configuration.CPUS));
        offerBuilder.addResources(mem(Configuration.MEM));
        offerBuilder.addResources(disk(Configuration.DISK));
        offerBuilder.addResources(portRange(Configuration.BEGIN_PORT, Configuration.END_PORT));

        scheduler.resourceOffers(driver, Collections.singletonList(offerBuilder.build()));

        OfferIDMatcher offerIdMatcher1 = new OfferIDMatcher("offer1");
        TaskInfoMatcher taskInfoMatcher1 = new TaskInfoMatcher("elasticsearch_host1_20150306T102040.789Z")
                                                        .slaveId("slave1")
                .cpus(Configuration.CPUS)
                .mem(Configuration.MEM)
                .disk(Configuration.DISK)
                .beginPort(31000)
                .endPort(31001);

        Mockito.verify(driver).launchTasks((Collection<Protos.OfferID>) Mockito.argThat(org.hamcrest.Matchers.contains(offerIdMatcher1)), (Collection<Protos.TaskInfo>) Mockito.argThat(org.hamcrest.Matchers.contains(taskInfoMatcher1)));
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
