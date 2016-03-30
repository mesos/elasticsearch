package org.apache.mesos.elasticsearch.scheduler;

import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.common.cli.ZookeeperCLIParameter;
import org.apache.mesos.elasticsearch.scheduler.state.ClusterState;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

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

    @Test
    public void shouldGenerateValidNativeCommand() {
        Configuration configuration = new Configuration(ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "aa");
        final List<String> arguments = Arrays.asList("test1", "test2");

        final String nativeCommand = configuration.nativeCommand(arguments);
        assertTrue(nativeCommand.contains(arguments.get(0)));
        assertTrue(nativeCommand.contains(arguments.get(1)));
        assertTrue(nativeCommand.contains("bin/elasticsearch"));
        assertTrue(nativeCommand.contains("chown"));
    }

    @Test
    public void shouldCreateArguments() {
        Configuration configuration = new Configuration(ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "aa");
        final ClusterState clusterState = Mockito.mock(ClusterState.class);
        final int port = 1234;
        final Protos.DiscoveryInfo discoveryInfo = Protos.DiscoveryInfo.newBuilder().setPorts(Protos.Ports.newBuilder()
                .addPorts(Protos.Port.newBuilder().setNumber(port))
                .addPorts(Protos.Port.newBuilder().setNumber(port)))
                .setVisibility(Protos.DiscoveryInfo.Visibility.EXTERNAL)
                .build();
        Protos.SlaveID slaveID = Protos.SlaveID.newBuilder().setValue("SLAVE").build();
        final List<String> arguments = configuration.esArguments(clusterState, discoveryInfo, slaveID);
        String allArgs = arguments.toString();
        assertTrue(allArgs.contains(Integer.toString(port)));
    }

    @Test
    public void shouldCreateVolumeName() {
        Configuration configuration = new Configuration(ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "aa", Configuration.FRAMEWORK_NAME, "test");
        assertEquals("test0data", configuration.dataVolumeName(0L));
    }

    @Test
    public void shouldCreateTaskLabels() {
        Configuration configuration = new Configuration(
            ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "aa", Configuration.EXECUTOR_LABELS,
              "foo=bar",
              "incomplete",
              "empty=",
              "separator=values=are=joined");
        Map<String, String> labels = configuration.getTaskLabels();

        assertEquals("bar", labels.get("foo"));
        assertFalse(labels.containsKey("incomplete"));
        assertEquals("", labels.get("empty"));
        assertEquals("values=are=joined", labels.get("separator"));
    }
}
