package org.apache.mesos.elasticsearch.scheduler;

import com.beust.jcommander.ParameterException;
import org.apache.mesos.elasticsearch.common.cli.ElasticsearchCLIParameter;
import org.apache.mesos.elasticsearch.common.cli.ZookeeperCLIParameter;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * CLI Tests
 */
@SuppressWarnings("PMD.TooManyMethods")
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
        String[] args = {ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "zk://dummyIPAddress:2181"};
        new Configuration(args);
    }

    @Test(expected = com.beust.jcommander.ParameterException.class)
    public void shouldCrashIfNoRequiredParams() {
        String[] args = {};
        new Configuration(args);
    }
    
    @Test
    public void randomParamTest() {
        String[] args = {Configuration.ELASTICSEARCH_RAM, "512", ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "zk://dummyIPAddress:2181"};
        Configuration configuration = new Configuration(args);
        assertEquals(512.0, configuration.getMem(), 0.1);
    }

    @Test(expected = com.beust.jcommander.ParameterException.class)
    public void shouldRejectZookeeperMesosTimeoutEqualTo0() {
        String[] args = {ZookeeperCLIParameter.ZOOKEEPER_MESOS_TIMEOUT, "0", ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "zk://dummyIPAddress:2181"};
        new Configuration(args);
    }

    @Test(expected = com.beust.jcommander.ParameterException.class)
    public void shouldRejectZookeeperMesosTimeoutLessThan0() {
        String[] args = {ZookeeperCLIParameter.ZOOKEEPER_MESOS_TIMEOUT, "-1", ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "zk://dummyIPAddress:2181"};
        new Configuration(args);
    }

    @Test(expected = com.beust.jcommander.ParameterException.class)
    public void shouldRejectZookeeperFrameworkTimeoutEqualTo0() {
        String[] args = {ZookeeperCLIParameter.ZOOKEEPER_FRAMEWORK_TIMEOUT, "0", ZookeeperCLIParameter.ZOOKEEPER_FRAMEWORK_URL, "zk://dummyIPAddress:2181"};
        new Configuration(args);
    }

    @Test(expected = com.beust.jcommander.ParameterException.class)
    public void shouldRejectZookeeperFrameworkTimeoutLessThan0() {
        String[] args = {ZookeeperCLIParameter.ZOOKEEPER_FRAMEWORK_TIMEOUT, "-1", ZookeeperCLIParameter.ZOOKEEPER_FRAMEWORK_URL, "zk://dummyIPAddress:2181"};
        new Configuration(args);
    }

    @Test(expected = com.beust.jcommander.ParameterException.class)
    public void shouldFailIfExecutorTimeoutLessThanHealthDelay() {
        String[] args = {Configuration.EXECUTOR_HEALTH_DELAY, "1000", Configuration.EXECUTOR_TIMEOUT, "10", ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "zk://dummyIPAddress:2181"};
        new Configuration(args);
    }

    @Test(expected = com.beust.jcommander.ParameterException.class)
    public void shouldFailIfParamIsEmpty() {
        String[] args = {ElasticsearchCLIParameter.ELASTICSEARCH_CLUSTER_NAME, " ", ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "zk://dummyIPAddress:2181"};
        new Configuration(args);
    }

    @Test
    public void shouldBooleanOk() {
        String[] args = {Configuration.EXECUTOR_FORCE_PULL_IMAGE, "true", ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "zk://dummyIPAddress:2181"};
        Configuration configuration = new Configuration(args);
        assertTrue(configuration.getExecutorForcePullImage());
    }

    @Test
    public void shouldHandleSpaces() {
        String[] args = {Configuration.EXECUTOR_FORCE_PULL_IMAGE, "  true ", ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "zk://dummyIPAddress:2181"};
        Configuration configuration = new Configuration(args);
        assertTrue(configuration.getExecutorForcePullImage());
    }

    @Test(expected = com.beust.jcommander.ParameterException.class)
    public void shouldCrashIfInvalidBoolean2() {
        String[] args = {Configuration.EXECUTOR_FORCE_PULL_IMAGE, "0", ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "zk://dummyIPAddress:2181"};
        new Configuration(args);
    }

    @Test(expected = com.beust.jcommander.ParameterException.class)
    public void shouldCrashIfInvalidBoolean3() {
        String[] args = {Configuration.EXECUTOR_FORCE_PULL_IMAGE, "afds", ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "zk://dummyIPAddress:2181"};
        new Configuration(args);
    }

    @Test(expected = com.beust.jcommander.ParameterException.class)
    public void doesNotAcceptBlank() {
        String[] args = {ElasticsearchCLIParameter.ELASTICSEARCH_SETTINGS_LOCATION, "", ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "zk://dummyIPAddress:2181"};
        new Configuration(args);
    }

    @Test
    public void shouldAcceptTimeoutGreaterThanHealthDelay() {
        String[] args = {
                Configuration.EXECUTOR_HEALTH_DELAY, "100",
                Configuration.EXECUTOR_TIMEOUT, "5000",
                ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "zk://dummyIPAddress:2181"};
        new Configuration(args);
    }

    @Test(expected = com.beust.jcommander.ParameterException.class)
    public void orderingOfParametersIsImportant() {
        String[] args = {
                Configuration.EXECUTOR_HEALTH_DELAY, "5000", // This won't work, because EXECUTOR_TIMEOUT is still the default 30000. I.e. (4999 !> 300000)
                Configuration.EXECUTOR_TIMEOUT, "3000",
                ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "zk://dummyIPAddress:2181"};
        new Configuration(args);
    }

    @Test
    public void shouldUseMesosZKURLIfFrameworkZKURLNotSupplied() {
        String frameworkFormattedZKURL = "dummyIPAddress:2181";
        String[] args = {ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "zk://" + frameworkFormattedZKURL};
        Configuration configuration = new Configuration(args);
        assertEquals(frameworkFormattedZKURL, configuration.getFrameworkZKURL());
    }

    @Test
    public void shouldReturnBlankIfDefaultFrameworkZKURL() {
        String frameworkFormattedZKURL = "dummyIPAddress:2181";
        String[] args = {ZookeeperCLIParameter.ZOOKEEPER_FRAMEWORK_URL, ZookeeperCLIParameter.DEFAULT, ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "zk://" + frameworkFormattedZKURL};
        Configuration configuration = new Configuration(args);
        assertEquals(frameworkFormattedZKURL, configuration.getFrameworkZKURL());
    }

    @Test
    public void shouldParseValidPorts() {
        String validPorts = "9200,9300";
        Configuration configuration = new Configuration(ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "aa", Configuration.ELASTICSEARCH_PORTS, validPorts);
        List<Integer> elasticsearchPorts = configuration.getElasticsearchPorts();
        assertEquals(9200, elasticsearchPorts.get(0).intValue());
        assertEquals(9300, elasticsearchPorts.get(1).intValue());
    }

    @Test
    public void shouldBeOkWithSpaces() {
        String validPorts = "9200, 9300";
        Configuration configuration = new Configuration(ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "aa", Configuration.ELASTICSEARCH_PORTS, validPorts);
        List<Integer> elasticsearchPorts = configuration.getElasticsearchPorts();
        assertEquals(9200, elasticsearchPorts.get(0).intValue());
        assertEquals(9300, elasticsearchPorts.get(1).intValue());
    }

    @Test(expected = ParameterException.class)
    public void shouldNotAcceptSinglePort() {
        String validPorts = "9200";
        new Configuration(ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "aa", Configuration.ELASTICSEARCH_PORTS, validPorts);
    }

    @Test(expected = ParameterException.class)
    public void shouldNotAcceptNonIntegers() {
        String validPorts = "9200,abc";
        new Configuration(ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "aa", Configuration.ELASTICSEARCH_PORTS, validPorts);
    }

    @Test(expected = ParameterException.class)
    public void shouldNotAcceptMultiplePorts() {
        String validPorts = "9200,9300,9400";
        new Configuration(ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "aa", Configuration.ELASTICSEARCH_PORTS, validPorts);
    }

    @Test(expected = com.beust.jcommander.ParameterException.class)
    public void printHelpWithValidDefaults() {
        new Configuration(new String[]{});
    }
}