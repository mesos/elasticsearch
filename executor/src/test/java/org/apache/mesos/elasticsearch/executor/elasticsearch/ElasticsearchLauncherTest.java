package org.apache.mesos.elasticsearch.executor.elasticsearch;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;


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
    public void shouldLaunchWithValidSettings() throws InterruptedException {
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
}