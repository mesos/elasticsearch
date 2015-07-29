package org.apache.mesos.elasticsearch.scheduler.healthcheck;

/**
 * A healthcheck for a single executor
 */
public class ExecutorHealthCheck {
    private final PollService pollService;
    private final Shutdown shutdownThread;

    public ExecutorHealthCheck(PollService pollService) {
        this.pollService = pollService;
        pollService.start();
        shutdownThread = new Shutdown();
        Runtime.getRuntime().addShutdownHook(shutdownThread);
    }

    public void stopHealthcheck() {
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
