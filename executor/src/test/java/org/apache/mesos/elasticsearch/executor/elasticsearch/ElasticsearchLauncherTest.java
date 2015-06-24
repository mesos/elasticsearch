package org.apache.mesos.elasticsearch.executor.elasticsearch;

import org.apache.mesos.elasticsearch.executor.model.PortsModel;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Mockito.*;


/**
 * Tests that ES node can be launched using provided settings.
 */
@RunWith(MockitoJUnitRunner.class)
public class ElasticsearchLauncherTest {
    @Test(expected = NullPointerException.class)
    public void shouldExceptionIfNullSettings() {
        new ElasticsearchLauncher(null);
    }

    @Test
    public void shouldLaunchWithDefaultSettings() throws InterruptedException {
        ElasticsearchSettings esSettings = new ElasticsearchSettings();
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

    @Test
    public void shouldLaunchWithRunTimeSettings() throws InterruptedException {
        ElasticsearchSettings esSettings = new ElasticsearchSettings();
        ImmutableSettings.Builder settings = esSettings.defaultSettings()
                // Set local
                .put("node.local", true);
        // Remove zookeeper settings
        settings.remove("discovery.type");
        settings.remove("sonian.elasticsearch.zookeeper.settings.enabled");
        settings.remove("sonian.elasticsearch.zookeeper.client.host");
        settings.remove("sonian.elasticsearch.zookeeper.discovery.state_publishing.enabled");
        ElasticsearchLauncher elasticsearchLauncher = new ElasticsearchLauncher(settings);
        elasticsearchLauncher.addRuntimeSettings(getClientPort());
        elasticsearchLauncher.launch();
    }

    @Test
    public void shouldBeAbleToAddRunTimeSettings() {
        // Given settings
        ImmutableSettings.Builder settings = mock(ImmutableSettings.Builder.class);
        Launcher launcher = new ElasticsearchLauncher(settings);

        // When add runtime
        ImmutableSettings.Builder runtimeSettings = spy(getClientPort());
        launcher.addRuntimeSettings(runtimeSettings);

        // Ensure settings are updated
        verify(settings, times(1)).put(runtimeSettings.build());
    }

    private ImmutableSettings.Builder getClientPort() {
        return ImmutableSettings.settingsBuilder().put(PortsModel.HTTP_PORT_KEY, "1234");

    }
}