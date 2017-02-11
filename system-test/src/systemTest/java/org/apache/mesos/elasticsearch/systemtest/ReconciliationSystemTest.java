package org.apache.mesos.elasticsearch.systemtest;

import com.containersol.minimesos.docker.DockerClientFactory;
import com.github.dockerjava.api.DockerClient;
import org.apache.mesos.elasticsearch.systemtest.base.TestBase;
import org.apache.mesos.elasticsearch.systemtest.containers.ElasticsearchSchedulerContainer;
import org.apache.mesos.elasticsearch.systemtest.util.ContainerLifecycleManagement;
import org.apache.mesos.elasticsearch.systemtest.util.DockerUtil;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static com.jayway.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;

/**
 * Tests CLUSTER state monitoring and reconciliation.
 */
@SuppressWarnings({"PMD.TooManyMethods"})
@Ignore("This test has to be merged into DeploymentSystemTest. See https://github.com/mesos/elasticsearch/issues/591")
public class ReconciliationSystemTest extends TestBase {
    private static final int TIMEOUT = 120;
    private static final ContainerLifecycleManagement CONTAINER_MANAGER = new ContainerLifecycleManagement();
    private static DockerClient dockerClient = DockerClientFactory.build();
    private DockerUtil dockerUtil = new DockerUtil(dockerClient);

    private static ElasticsearchSchedulerContainer startSchedulerContainer() {
        ElasticsearchSchedulerContainer scheduler = new ElasticsearchSchedulerContainer(dockerClient, CLUSTER.getZooKeeper().getIpAddress());
        CONTAINER_MANAGER.addAndStart(scheduler, TEST_CONFIG.getClusterTimeout());
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

    private void assertCorrectNumberOfExecutors(int expected) throws IOException {
        await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> dockerUtil.getExecutorContainers().size() == expected);
        assertEquals(expected, dockerUtil.getExecutorContainers().size());
    }
}
