package org.apache.mesos.elasticsearch.scheduler.healthcheck;

import org.apache.log4j.Logger;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Provides the polling mechanism
 */
public class PollService {
    private static final Logger LOGGER = Logger.getLogger(PollService.class);
    private final Runnable poller;
    private final Long delay;
    private ScheduledFuture future;
    private ScheduledExecutorService executor;

    /**
     * New polling service
     * @param poller A Runnable that represents your polling mechanism
     * @param delay the delay in milliseconds
     */
    public PollService(Runnable poller, Long delay) {
        this.poller = poller;
        this.delay = delay;
    }

    /**
     * Starts the polling service. Note: It is *your* responsibility to stop the process by calling stop() before shutdown.
     */
    public void start() {
        LOGGER.debug("Starting poll service");
        executor = Executors.newSingleThreadScheduledExecutor();
        future = executor.scheduleWithFixedDelay(poller, 0, delay, TimeUnit.MILLISECONDS);
    }

    /**
     * Stops the polling service. It is *your* responsibility to stop the process before shutdown.
     */
    public void stop() {
        LOGGER.debug("Stopping poll service");
        if (future != null) {
            future.cancel(false);
        }
        if (executor != null) {
            executor.shutdown();
        }
    }
}
