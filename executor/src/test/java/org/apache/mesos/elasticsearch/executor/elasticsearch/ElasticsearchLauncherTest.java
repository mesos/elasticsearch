package org.apache.mesos.elasticsearch.executor.elasticsearch;

import org.apache.mesos.elasticsearch.common.cli.ElasticsearchCLIParameter;
import org.apache.mesos.elasticsearch.executor.Configuration;
import org.apache.mesos.elasticsearch.executor.model.PortsModel;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertNotNull;
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
        Settings.Builder settings = esSettings.defaultSettings()
                .put("node.local", true)
                .put("path.home", ".");
        ElasticsearchLauncher elasticsearchLauncher = new ElasticsearchLauncher(settings);
        elasticsearchLauncher.launch();
    }

    @Test
    public void shouldLaunchWithRunTimeSettings() throws InterruptedException {
        ElasticsearchSettings esSettings = new ElasticsearchSettings();
        Settings.Builder settings = esSettings.defaultSettings()
                .put("node.local", true)
                .put("path.home", ".");
        ElasticsearchLauncher elasticsearchLauncher = new ElasticsearchLauncher(settings);
        elasticsearchLauncher.addRuntimeSettings(getClientPort());
        elasticsearchLauncher.launch();
    }

    @Test
    public void shouldBeAbleToAddRunTimeSettings() {
        // Given settings
        Settings.Builder settings = mock(Settings.Builder.class);
        Launcher launcher = new ElasticsearchLauncher(settings);

        // When add runtime
        Settings.Builder runtimeSettings = spy(getClientPort());
        launcher.addRuntimeSettings(runtimeSettings);

        // Ensure settings are updated
        verify(settings, times(1)).put(runtimeSettings.build());
    }

    @Test
    public void shouldBeAbleToLoadSettingsFromResources() {
        Configuration configuration = new Configuration(new String[]{""});
        Settings.Builder esSettings = configuration.getElasticsearchYmlSettings();
        assertNotNull(esSettings);
    }

    @Test
    public void shouldBeAbleToLoadSettingsFromFile() throws IOException {
        // Create temp file
        Path tempFile = Files.createTempFile("elasticsearchTest", ".yml");
        Settings.Builder esSettings;
        try {
            Configuration configuration = new Configuration(new String[]{ElasticsearchCLIParameter.ELASTICSEARCH_SETTINGS_LOCATION, tempFile.toString()});
            esSettings = configuration.getElasticsearchYmlSettings();
        } finally {
            Files.delete(tempFile);
        }
        assertNotNull(esSettings);
    }

    private Settings.Builder getClientPort() {
        return Settings.settingsBuilder().put(PortsModel.HTTP_PORT_KEY, "1234");

    }
}