package org.apache.mesos.elasticsearch.scheduler.healthcheck;

import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.jayway.awaitility.Awaitility.await;
import static org.junit.Assert.*;

/**
 * Test poll service
 */
public class PollServiceTest {
    private static final AtomicInteger counter = new AtomicInteger(0);
    @Test
    public void testPollService() {
        Runnable pollerRunnable = () -> counter.incrementAndGet();
        PollService pollService = new PollService(pollerRunnable, 1L);
        pollService.start();
        await().atMost(1, TimeUnit.SECONDS).until(() -> counter.get() > 0);
        assertTrue(counter.get() > 0);
        pollService.stop();
    }
}