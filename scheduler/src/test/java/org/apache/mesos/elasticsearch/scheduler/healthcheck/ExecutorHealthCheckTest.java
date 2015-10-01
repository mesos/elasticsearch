package org.apache.mesos.elasticsearch.scheduler.healthcheck;

import com.jayway.awaitility.Awaitility;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests healthcheck mechanism.
 */
public class ExecutorHealthCheckTest {

    /**
     * Test without scheduler. Simple mocked healthcheck.
     * Tests whole healthcheck chain.
     */
    @Test
    public void testHTTPHealthCheck() throws InterruptedException {
        HTTPHealthCheck httpHealthCheck = new HTTPHealthCheck();
        ExecutorHealthCheck healthCheck = new ExecutorHealthCheck(new PollService(httpHealthCheck, 100L));
        Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS)
                .until(() -> httpHealthCheck.counter.get() > 0);
        assertTrue(httpHealthCheck.counter.get() > 0);

        // Stop healthcheck, wait for a bit, and make sure we have stopped sending updates.
        healthCheck.stopHealthcheck(); // It might be in the middle of a poll, so might take a bit to shut down.
        httpHealthCheck.counter.set(0);
        Thread.sleep(100L); // Wait, to see if it updates again. It shold not, because we told it to stop.
        assertEquals(0, httpHealthCheck.counter.get());
    }

    private static class HTTPHealthCheck implements Runnable {
        public volatile AtomicInteger counter = new AtomicInteger(0);

        @Override
        public void run() {
            counter.incrementAndGet();
        }
    }

}


