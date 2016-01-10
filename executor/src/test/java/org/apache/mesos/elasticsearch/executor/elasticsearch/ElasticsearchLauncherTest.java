package org.apache.mesos.elasticsearch.executor.elasticsearch;

import com.jayway.awaitility.Awaitility;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.log4j.Logger;
import org.apache.mesos.elasticsearch.common.cli.ElasticsearchCLIParameter;
import org.apache.mesos.elasticsearch.executor.Configuration;
import org.apache.mesos.elasticsearch.executor.model.PortsModel;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Tests that ES node can be launched using provided settings.
 */
public class ElasticsearchLauncherTest {
    private static final Logger LOG = Logger.getLogger(ElasticsearchLauncherTest.class);

    @Test(expected = NullPointerException.class)
    public void shouldExceptionIfNullSettings() {
        new ElasticsearchLauncher(null);
    }

    @Test
    public void shouldLaunchWithDefaultSettings() throws InterruptedException, UnirestException, ExecutionException {
        ElasticsearchSettings esSettings = new ElasticsearchSettings();
        Settings.Builder settings = esSettings.defaultSettings()
                .put("node.local", true)
                .put("path.data", ".")
                .put("path.home", ".");
        ElasticsearchLauncher elasticsearchLauncher = new ElasticsearchLauncher(settings);
        safeStartAndShutdownNode(elasticsearchLauncher, nodeConsumer(9200));
    }

    @Test
    public void shouldShutdownNode() {
        Node node = NodeBuilder.nodeBuilder().settings(Settings.settingsBuilder().put("path.home", ".").build()).node();
        node.close();
        assertTrue(node.isClosed());
    }

    @Test
    public void shouldLaunchWithRunTimeSettings() throws InterruptedException, ExecutionException, UnirestException {
        ElasticsearchSettings esSettings = new ElasticsearchSettings();
        Settings.Builder settings = esSettings.defaultSettings()
                .put("node.local", true)
                .put("path.data", ".")
                .put("path.home", ".");
        ElasticsearchLauncher elasticsearchLauncher = new ElasticsearchLauncher(settings);
        Integer port = 1234;
        elasticsearchLauncher.addRuntimeSettings(clientPortSetting(port));
        safeStartAndShutdownNode(elasticsearchLauncher, nodeConsumer(port));
    }

    @Test
    public void shouldBeAbleToAddRunTimeSettings() {
        // Given settings
        Settings.Builder settings = mock(Settings.Builder.class);
        Launcher launcher = new ElasticsearchLauncher(settings);

        // When add runtime
        Settings.Builder runtimeSettings = spy(clientPortSetting(1234));
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

    private Settings.Builder clientPortSetting(Integer http) {
        return Settings.settingsBuilder().put(PortsModel.HTTP_PORT_KEY, http);

    }

    public Consumer<Node> nodeConsumer(Integer port) throws InterruptedException, ExecutionException, UnirestException {
        return node -> {
            try {
                debugNodeInfo(node);
                assertStarted(port);
            } catch (InterruptedException | ExecutionException | UnirestException e) {
                e.printStackTrace();
            }
        };
    }

    public void safeStartAndShutdownNode(ElasticsearchLauncher launcher, Consumer<Node> function) throws InterruptedException {
        Node node = launcher.launch();
        try {
            function.accept(node);
        } finally {
            if (node != null) {
                node.close();
                Awaitility.await().pollInterval(1L, TimeUnit.SECONDS).atMost(30L, TimeUnit.SECONDS).until(node::isClosed);
                assertTrue("Node did not close properly. Check ps -ax for rouge elasticsearch processes.", node.isClosed());
            }
        }
    }

    public void debugNodeInfo(Node node) throws InterruptedException, ExecutionException {
        node.settings().getAsMap().forEach((s, s2) -> LOG.debug(s + " = " + s2));
        LOG.debug(node.isClosed());
        ActionFuture<ClusterStateResponse> state = node.client().admin().cluster().state(new ClusterStateRequest());
        LOG.debug(state.get().getState().prettyPrint());
    }

    public void assertStarted(Integer port) throws UnirestException {
        waitForStart(port);
        LOG.debug(Unirest.get("http://127.0.0.1:" + port).asString().getBody());
        assertEquals(200, Unirest.get("http://127.0.0.1:" + port).asJson().getStatus());
    }

    public void waitForStart(Integer port) {
        Awaitility.await().pollInterval(1L, TimeUnit.SECONDS).atMost(30L, TimeUnit.SECONDS).until(() -> {
            try {
                return Unirest.get("http://127.0.0.1:" + port).asJson().getStatus() == 200;
            } catch (UnirestException e) {
                return false;
            }
        });
    }
}