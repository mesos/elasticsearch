package org.apache.mesos.elasticsearch.executor.elasticsearch;

import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.executor.model.PortsModel;
import org.apache.mesos.elasticsearch.executor.model.ZooKeeperModel;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Tests that ES node can be launched using provided settings.
 */
@RunWith(MockitoJUnitRunner.class)
public class ElasticsearchLauncherTest {
    @Mock
    private ZooKeeperModel zooKeeperModel;
    @Mock
    private PortsModel portsModel;

    @Before
    public void setup() {
        Protos.Port clientPort = Protos.Port.newBuilder().setNumber(9200).build();
        Protos.Port transportPort = Protos.Port.newBuilder().setNumber(9300).build();
        when(portsModel.getClientPort()).thenReturn(clientPort);
        when(portsModel.getTransportPort()).thenReturn(transportPort);
        when(zooKeeperModel.getAddress()).thenReturn("localhost");
    }

    @Test(expected = NullPointerException.class)
    public void shouldExceptionIfNullSettings() {
        new ElasticsearchLauncher(null);
    }

    @Test
    public void shouldLaunchWithValidSettings() throws InterruptedException {
        ElasticsearchSettings esSettings = new ElasticsearchSettings(portsModel, zooKeeperModel);
        ImmutableSettings.Builder settings = esSettings.defaultSettings()
                // Set local
                .put("node.local", true);
        // Remove zookeeper settings
        settings.remove("discovery.type");
        settings.remove("sonian.elasticsearch.zookeeper.settings.enabled");
        settings.remove("sonian.elasticsearch.zookeeper.client.host");
        settings.remove("sonian.elasticsearch.zookeeper.discovery.state_publishing.enabled");
        ElasticsearchLauncher elasticsearchLauncher = new ElasticsearchLauncher(settings);
        elasticsearchLauncher.launch();
    }
}