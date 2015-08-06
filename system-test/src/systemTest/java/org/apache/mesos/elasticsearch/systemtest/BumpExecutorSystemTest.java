package org.apache.mesos.elasticsearch.systemtest;

import com.jayway.awaitility.Awaitility;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.log4j.Logger;
import org.apache.mesos.elasticsearch.scheduler.healthcheck.ExecutorHealthCheck;
import org.apache.mesos.elasticsearch.scheduler.healthcheck.PollService;
import org.apache.mesos.mini.MesosCluster;
import org.apache.mesos.mini.mesos.MesosClusterConfig;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.http.HttpStatus;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests healthcheck mechanism.
 */
public class BumpExecutorSystemTest {
    private static final Logger LOGGER = Logger.getLogger(BumpExecutorSystemTest.class);

    protected static final MesosClusterConfig CONFIG = MesosClusterConfig.builder()
            .numberOfSlaves(3)
            .privateRegistryPort(15000) // Currently you have to choose an available port by yourself
            .slaveResources(new String[]{"ports(*):[9200-9200,9300-9300]", "ports(*):[9201-9201,9301-9301]", "ports(*):[9202-9202,9302-9302]"})
            .build();

    @ClassRule
    public static final MesosCluster CLUSTER = new MesosCluster(CONFIG);

    /**
     * Test without scheduler. Simple HTTP check
     */
    @Test
    public void testHTTPHealthCheck() throws InterruptedException {
        HTTPHealthCheck httpHealthCheck = new HTTPHealthCheck("http://" + CLUSTER.getMesosContainer().getMesosMasterURL());
        ExecutorHealthCheck healthCheck = new ExecutorHealthCheck(new PollService(httpHealthCheck, 100L));
        Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS)
                .until(() -> httpHealthCheck.counter.get() > 0);
        assertTrue(httpHealthCheck.counter.get() > 0);

        // Stop healthcheck, wait for a bit, and make sure we have stopped sending updates.
        healthCheck.stopHealthcheck(); // It might be in the middle of a poll, so might take a bit to shut down.
        Thread.sleep(1000L);
        httpHealthCheck.counter.set(0);
        Thread.sleep(1000L);
        assertEquals(0, httpHealthCheck.counter.get());
    }

    private static class HTTPHealthCheck implements Runnable {
        private final String url;
        public volatile AtomicInteger counter = new AtomicInteger(0);

        public HTTPHealthCheck(String url) {
            this.url = url;
        }

        @Override
        public void run() {
            try {
                Unirest.setTimeouts(1000L, 1000L);
                Integer status = Unirest.get(url).asString().getStatus();
                LOGGER.debug("Status = " + status);
                if (status == HttpStatus.OK.value()) {
                    counter.incrementAndGet();
                }
            } catch (UnirestException e) {
                LOGGER.error("Could not ping " + url, e);
            }

        }
    }

}


