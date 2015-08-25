package org.apache.mesos.elasticsearch.scheduler;

import org.apache.mesos.elasticsearch.common.zookeeper.ZookeeperCLIParameter;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;

/**
 * CLI Tests
 */
public class CLITest {
    private Environment env = Mockito.mock(Environment.class);

    @Before
    public void before() {
        Mockito.when(env.getJavaHeap()).thenReturn("dummy");
    }

    @Test(expected = com.beust.jcommander.ParameterException.class)
    public void mainShouldBombOutOfMainIfInvalidParams() {
        String[] args = {};
        Main main = new Main(env);
        main.run(args);
    }

    @Test
    public void shouldPassIfOnlyRequiredParams() {
        String[] args = {ZookeeperCLIParameter.ZOOKEEPER_URL, "zk://dummyIPAddress:2181"};
        new Configuration(args);
    }

    @Test(expected = com.beust.jcommander.ParameterException.class)
    public void shouldCrashIfNoRequiredParams() {
        String[] args = {};
        new Configuration(args);
    }
    
    @Test
    public void randomParamTest() {
        String[] args = {Configuration.ELASTICSEARCH_RAM, "512", ZookeeperCLIParameter.ZOOKEEPER_URL, "zk://dummyIPAddress:2181"};
        Configuration configuration = new Configuration(args);
        assertEquals(512.0, configuration.getMem(), 0.1);
    }

    @Test(expected = com.beust.jcommander.ParameterException.class)
    public void shouldRejectNumbersEqualTo0() {
        String[] args = {ZookeeperCLIParameter.ZOOKEEPER_TIMEOUT, "0", ZookeeperCLIParameter.ZOOKEEPER_URL, "zk://dummyIPAddress:2181"};
        new Configuration(args);
    }

    @Test(expected = com.beust.jcommander.ParameterException.class)
    public void shouldRejectNumbersLessThan0() {
        String[] args = {ZookeeperCLIParameter.ZOOKEEPER_TIMEOUT, "-1", ZookeeperCLIParameter.ZOOKEEPER_URL, "zk://dummyIPAddress:2181"};
        new Configuration(args);
    }

    @Test(expected = com.beust.jcommander.ParameterException.class)
    public void shouldFailIfExecutorTimeoutLessThanHealthDelay() {
        String[] args = {Configuration.EXECUTOR_HEALTH_DELAY, "1000", Configuration.EXECUTOR_TIMEOUT, "10", ZookeeperCLIParameter.ZOOKEEPER_URL, "zk://dummyIPAddress:2181"};
        new Configuration(args);
    }

    @Test(expected = com.beust.jcommander.ParameterException.class)
    public void shouldFailIfParamIsEmpty() {
        String[] args = {Configuration.ELASTICSEARCH_CLUSTER_NAME, " ", ZookeeperCLIParameter.ZOOKEEPER_URL, "zk://dummyIPAddress:2181"};
        new Configuration(args);
    }
}