package org.apache.mesos.elasticsearch.scheduler.healthcheck;

import org.apache.log4j.Logger;

/**
 * A healthcheck for a single executor
 */
public class ExecutorHealthCheck {
    private static final Logger LOGGER = Logger.getLogger(ExecutorHealthCheck.class);
    private final PollService pollService;
    private final Shutdown shutdownThread;

    public ExecutorHealthCheck(PollService pollService) {
        this.pollService = pollService;
        LOGGER.debug("Starting healthcheck");
        pollService.start();
        shutdownThread = new Shutdown();
        Runtime.getRuntime().addShutdownHook(shutdownThread);
    }

    public void stopHealthcheck() {
        LOGGER.debug("Stopping healthcheck");
        pollService.stop();
        Runtime.getRuntime().removeShutdownHook(shutdownThread);
    }

    private class Shutdown extends Thread {
        @Override
        public void run() {
            pollService.stop();
        }
    }
}
