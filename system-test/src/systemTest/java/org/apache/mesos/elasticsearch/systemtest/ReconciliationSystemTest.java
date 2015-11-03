package org.apache.mesos.elasticsearch.systemtest;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import org.apache.mesos.elasticsearch.common.cli.ElasticsearchCLIParameter;
import org.apache.mesos.elasticsearch.common.cli.ZookeeperCLIParameter;
import org.apache.mesos.elasticsearch.scheduler.Configuration;
import org.apache.mesos.elasticsearch.systemtest.base.TestBase;
import org.apache.mesos.elasticsearch.systemtest.util.ContainerLifecycleManagement;
import org.apache.mesos.elasticsearch.systemtest.util.DockerUtil;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.concurrent.TimeUnit;

import static com.jayway.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests CLUSTER state monitoring and reconciliation.
 */
@SuppressWarnings({"PMD.TooManyMethods"})
public class ReconciliationSystemTest extends TestBase {
    private static final int TIMEOUT = 60;
    private static final ContainerLifecycleManagement CONTAINER_MANAGER = new ContainerLifecycleManagement();
    private DockerUtil dockerUtil = new DockerUtil(CLUSTER.getConfig().dockerClient);

    private static ElasticsearchSchedulerContainer startSchedulerContainer() {
        ElasticsearchSchedulerContainer scheduler = new ElasticsearchSchedulerContainer(CLUSTER.getConfig().dockerClient, CLUSTER.getZkContainer().getIpAddress());
        CONTAINER_MANAGER.addAndStart(scheduler);
        return scheduler;
    }

    @Rule
    public TestWatcher containerWatcher = new TestWatcher() {
        @Override
        protected void failed(Throwable e, Description description) {
            CONTAINER_MANAGER.stopAll();
        }
    };

    @After
    public void killContainers() {
        CONTAINER_MANAGER.stopAll();
    }

    @Test
    public void forceCheckExecutorTimeout() throws IOException {
        ElasticsearchSchedulerContainer scheduler = new TimeoutSchedulerContainer(CLUSTER.getConfig().dockerClient, CLUSTER.getZkContainer().getIpAddress());
        CONTAINER_MANAGER.addAndStart(scheduler);
        assertCorrectNumberOfExecutors(); // Start with 3
        assertLessThan(getTestConfig().getElasticsearchNodesCount()); // Then should be less than 3, because at some point we kill an executor
        assertCorrectNumberOfExecutors(); // Then at some point should get back to 3.
    }

    @Test
    public void ifSchedulerLostShouldReconcileExecutors() throws IOException {
        ElasticsearchSchedulerContainer scheduler = startSchedulerContainer();
        assertCorrectNumberOfExecutors();

        // Stop and restart container
        CONTAINER_MANAGER.stopContainer(scheduler);
        startSchedulerContainer();
        assertCorrectNumberOfExecutors();
    }

    @Test
    public void ifExecutorIsLostShouldStartAnother() throws IOException {
        startSchedulerContainer();
        assertCorrectNumberOfExecutors();

        dockerUtil.killOneExecutor();

        // Should restart an executor, so there should still be three
        assertCorrectNumberOfExecutors();

    }

    @Test
    public void ifExecutorIsLostWhileSchedulerIsDead() throws IOException {
        ElasticsearchSchedulerContainer scheduler = startSchedulerContainer();
        assertCorrectNumberOfExecutors();

        // Kill scheduler
        CONTAINER_MANAGER.stopContainer(scheduler);

        // Kill executor
        dockerUtil.killOneExecutor();

        startSchedulerContainer();
        // Should restart an executor, so there should still be three
        assertCorrectNumberOfExecutors();
    }

    private void assertCorrectNumberOfExecutors() throws IOException {
        assertCorrectNumberOfExecutors(getTestConfig().getElasticsearchNodesCount());
    }

    private void assertLessThan(int expected) throws IOException {
        await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> dockerUtil.getExecutorContainers().size() < expected);
        assertTrue(dockerUtil.getExecutorContainers().size() < expected);
    }

    private void assertCorrectNumberOfExecutors(int expected) throws IOException {
        await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> dockerUtil.getExecutorContainers().size() == expected);
        assertEquals(expected, dockerUtil.getExecutorContainers().size());
    }

    private static class TimeoutSchedulerContainer extends ElasticsearchSchedulerContainer {
        protected TimeoutSchedulerContainer(DockerClient dockerClient, String zkIp) {
            super(dockerClient, zkIp);
        }

        @Override
        protected CreateContainerCmd dockerCommand() {
            return dockerClient
                    .createContainerCmd(getTestConfig().getSchedulerImageName())
                    .withName(getTestConfig().getSchedulerName() + "_" + new SecureRandom().nextInt())
                    .withEnv("JAVA_OPTS=-Xms128m -Xmx256m")
                    .withCmd(
                            ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, getZookeeperMesosUrl(),
                            Configuration.EXECUTOR_HEALTH_DELAY, "99",
                            Configuration.EXECUTOR_TIMEOUT, "100", // This timeout is valid, but will always timeout, because of delays in receiving healthchecks.
                            ElasticsearchCLIParameter.ELASTICSEARCH_NODES, "3",
                            Configuration.ELASTICSEARCH_RAM, "256"
                    );
        }
    }

}
