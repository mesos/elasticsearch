package org.apache.mesos.elasticsearch.scheduler;

import org.apache.mesos.elasticsearch.common.cli.ZookeeperCLIParameter;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Tests
 **/
public class ConfigurationTest {
    @Test
    public void shouldReturnValidServerPath() throws UnknownHostException {
        Configuration configuration = new Configuration(ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "aa");
        String localhost = "localhost";
        int port = 1234;
        configuration.setFrameworkFileServerAddress(new InetSocketAddress(localhost, port));
        assertEquals("http://" + localhost + ":" + port, configuration.getFrameworkFileServerAddress());
    }

    @Test
    public void shouldNotHaveDefaultInetAddressToStringMethod() throws UnknownHostException {
        Configuration configuration = new Configuration(ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "aa");
        int port = 1234;
        configuration.setFrameworkFileServerAddress(new InetSocketAddress(InetAddress.getLocalHost().getHostName(), port));
        assertFalse(configuration.getFrameworkFileServerAddress().replace("http://", "").contains("/"));
    }

    @Test
    public void shouldProvideJavaHomeWithEndSlashAndWithoutJava() {
        Configuration configuration = new Configuration(ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "aa", Configuration.JAVA_HOME, "/usr/bin/java");
        assertEquals("/usr/bin/", configuration.getJavaHome());
        configuration = new Configuration(ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "aa", Configuration.JAVA_HOME, "/usr/bin/");
        assertEquals("/usr/bin/", configuration.getJavaHome());
        configuration = new Configuration(ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "aa", Configuration.JAVA_HOME, "/usr/bin");
        assertEquals("/usr/bin/", configuration.getJavaHome());
    }
}