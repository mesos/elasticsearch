package org.apache.mesos.elasticsearch.systemtest;

import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.model.Container;
import com.jayway.awaitility.Awaitility;
import org.apache.log4j.Logger;
import org.apache.mesos.elasticsearch.common.cli.ZookeeperCLIParameter;
import org.apache.mesos.elasticsearch.systemtest.base.TestBase;
import org.apache.mesos.elasticsearch.systemtest.callbacks.LogContainerTestCallback;
import org.apache.mesos.elasticsearch.systemtest.util.DockerUtil;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.Assert.assertTrue;

/**
 * Tests the main method.
 */
public class SchedulerMainSystemTest extends TestBase {
    private static final Logger LOGGER = Logger.getLogger(SchedulerMainSystemTest.class);
    private DockerUtil dockerUtil = new DockerUtil(CLUSTER.getConfig().dockerClient);

    @Test
    public void ensureMainFailsIfNoHeap() throws Exception {
        CreateContainerCmd createCommand = getCreateContainerCmd("");
        String containerId = startContainer(createCommand);
        waitForSchedulerStartStop(containerId);
        String containerLog = containerLog(containerId);
        LOGGER.debug(containerLog);
        assertTrue(containerLog.contains("Exception"));
        assertTrue(containerLog.contains("heap"));
    }

    @Test
    public void ensureMainFailsIfInvalidHeap() throws Exception {
        CreateContainerCmd createCommand = getCreateContainerCmd("-Xms128s1m -Xmx256f5m");
        String containerId = startContainer(createCommand);
        waitForSchedulerStartStop(containerId);
        String containerLog = containerLog(containerId);
        LOGGER.debug(containerLog);
        assertTrue(containerLog.contains("Invalid initial heap size"));
    }

    @Test
    public void ensureMainWorksIfValidHeap() throws Exception {
        CreateContainerCmd createCommand = getCreateContainerCmd("-Xms128m -Xmx256m");
        String containerId = startContainer(createCommand);
        waitFor(schedulerWithId(containerId));
    }

    private CreateContainerCmd getCreateContainerCmd(String heapString) {
        return CLUSTER.getConfig().dockerClient
                .createContainerCmd(getTestConfig().getSchedulerImageName())
                .withEnv("JAVA_OPTS=" + heapString)
                .withCmd(
                        ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "zk://" + "noIP" + ":2181/mesos"
                );
    }

    private String containerLog(String containerId) throws Exception {
        return CLUSTER.getConfig().dockerClient.logContainerCmd(containerId).withStdOut().withStdErr().withFollowStream().exec(new LogContainerTestCallback()).awaitCompletion().toString();
    }

    private String startContainer(CreateContainerCmd createCommand) {
        CreateContainerResponse r = createCommand.exec();
        String containerId = r.getId();
        CLUSTER.getConfig().dockerClient.startContainerCmd(containerId).exec();
        return containerId;
    }

    private void waitForSchedulerStartStop(String containerId) {
        waitFor(schedulerWithId(containerId));
        LOGGER.debug("Scheduler with id " + containerId + " started");
        waitFor(noSchedulers());
        LOGGER.debug("Scheduler stopped");
    }

    private Callable<Boolean> schedulerWithId(String containerId) {
        return () -> getContainerStream(true).anyMatch(container -> container.getId().equals(containerId));
    }

    private Callable<Boolean> noSchedulers() {
        return () -> getContainerStream(false).noneMatch(container -> container.getImage().contains("scheduler"));
    }

    private void waitFor(Callable<Boolean> callable) {
        Awaitility.await().atMost(30L, TimeUnit.SECONDS).until(callable);
    }

    private Stream<Container> getContainerStream(Boolean showAll) {
        return dockerUtil.getContainers(showAll).stream();
    }

}
