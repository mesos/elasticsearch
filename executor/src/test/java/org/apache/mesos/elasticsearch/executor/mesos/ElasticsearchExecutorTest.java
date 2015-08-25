package org.apache.mesos.elasticsearch.executor.mesos;

import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.common.Discovery;
import org.apache.mesos.elasticsearch.common.zookeeper.ZookeeperCLIParameter;
import org.apache.mesos.elasticsearch.executor.Configuration;
import org.apache.mesos.elasticsearch.executor.elasticsearch.Launcher;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * Tests for executor.
 * Major problem here is that the TaskInfo protocol buffer cannot be mocked with Mockito because it generates final classes.
 * Hence, we have to use generated TaskInfo packets, which means we have to add real data.
 */
@RunWith(MockitoJUnitRunner.class)
public class ElasticsearchExecutorTest {
    @Mock
    private Launcher launcher;

    @Mock
    private TaskStatus status;

    @InjectMocks
    private ElasticsearchExecutor executor;

    private String[] args = {ZookeeperCLIParameter.ZOOKEEPER_URL, "zk://dummy:2182"};

    @Spy
    private Configuration configuration = new Configuration(args);

    @Mock
    private ExecutorDriver driver;

    @Before
    public void setupLauncher() {
        Node node = mock(Node.class);
        when(launcher.launch()).thenReturn(node);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNPEIfNoTask() {
        executor.launchTask(driver, null);
    }

    @Test
    public void shouldSendStartUpdate() {
        // When launching
        executor.launchTask(driver, getDefaultTaskInfo().build());
        // Should send starting
        verify(status, times(1)).starting();
    }

    @Test
    public void shouldAddRuntimeSettings() {
        // E.g. should add ports, zookeeper address, etc.
        // When launching
        executor.launchTask(driver, getDefaultTaskInfo().build());
        // Should update settings
        verify(launcher, atLeastOnce()).addRuntimeSettings(any(ImmutableSettings.Builder.class));
    }

    @Test
    public void shouldLaunchNode() {
        // When launching
        executor.launchTask(driver, getDefaultTaskInfo().build());
        // Should call launch
        verify(launcher, atLeastOnce()).launch();
    }

    @Test
    public void shouldSendRunningUpdate() {
        // When launching
        executor.launchTask(driver, getDefaultTaskInfo().build());
        // Should send starting
        verify(status, times(1)).running();
    }

    private Protos.TaskInfo.Builder getDefaultTaskInfo() {
        return Protos.TaskInfo.newBuilder()
                .setName("")
                .setTaskId(Protos.TaskID.newBuilder().setValue("0"))
                .setSlaveId(Protos.SlaveID.newBuilder().setValue("0"))
                .setDiscovery(getDefaultDiscoveryInfo())
                .setExecutor(getDefaultExecutorInfo());
    }

    private Protos.DiscoveryInfo.Builder getDefaultDiscoveryInfo() {
        return Protos.DiscoveryInfo.newBuilder()
                .setPorts(Protos.Ports.newBuilder()
                        .addPorts(Protos.Port.newBuilder()
                                .setName(Discovery.CLIENT_PORT_NAME)
                                .setNumber(1234))
                        .addPorts(Protos.Port.newBuilder()
                                .setName(Discovery.TRANSPORT_PORT_NAME)
                                .setNumber(12345)))
                .setVisibility(Protos.DiscoveryInfo.Visibility.EXTERNAL);
    }

    private Protos.ExecutorInfo.Builder getDefaultExecutorInfo() {
        return Protos.ExecutorInfo.newBuilder()
                .setCommand(Protos.CommandInfo.newBuilder().addArguments(ZookeeperCLIParameter.ZOOKEEPER_URL).addArguments("zk://master:2181/mesos"))
                .setExecutorId(Protos.ExecutorID.newBuilder().setValue("0"));
    }
}