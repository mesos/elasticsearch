package org.apache.mesos.elasticsearch.scheduler;

import com.beust.jcommander.ParameterException;
import org.apache.mesos.elasticsearch.common.cli.ZookeeperCLIParameter;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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

    private static final Pattern PATTERN = Pattern.compile(
            "^(([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.){3}([01]?\\d\\d?|2[0-4]\\d|25[0-5])$");

    public static boolean validate(final String ip) {
        return PATTERN.matcher(ip).matches();
    }

    @Test
    public void shouldProvideIPAddress() {
        int port = 1234;
        Configuration configuration = new Configuration(ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "aa", Configuration.USE_IP_ADDRESS, "true");
        String string = configuration.addressToString(configuration.hostSocket(port));
        assertTrue(validate(string.replace("http://", "").replace(":" + port, "")));
    }


    @Test
    public void shouldProvideHostname() {
        int port = 1234;
        Configuration configuration = new Configuration(ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "aa");
        String string = configuration.addressToString(configuration.hostSocket(port));
        assertFalse(validate(string.replace("http://", "").replace(":" + port, "")));
    }
}