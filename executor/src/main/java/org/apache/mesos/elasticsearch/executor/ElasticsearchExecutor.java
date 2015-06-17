package org.apache.mesos.elasticsearch.executor;

import org.apache.log4j.Logger;
import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.common.Binaries;
import org.apache.mesos.elasticsearch.common.Discovery;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.plugins.PluginManager;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Executor for Elasticsearch.
 */
@SuppressWarnings("PMD.TooManyMethods")
public class ElasticsearchExecutor implements Executor {

    public static final Logger LOGGER = Logger.getLogger(ElasticsearchExecutor.class.toString());

    @Override
    public void registered(ExecutorDriver driver, Protos.ExecutorInfo executorInfo, Protos.FrameworkInfo frameworkInfo, Protos.SlaveInfo slaveInfo) {
        LOGGER.info("Executor Elasticsearch registered on slave " + slaveInfo.getHostname());
    }

    @Override
    public void reregistered(ExecutorDriver driver, Protos.SlaveInfo slaveInfo) {
        LOGGER.info("Executor Elasticsearch re-registered on slave " + slaveInfo.getHostname());
    }

    @Override
    public void disconnected(ExecutorDriver driver) {
        LOGGER.info("Executor Elasticsearch disconnected");
    }

    @Override
    public void launchTask(final ExecutorDriver driver, final Protos.TaskInfo task) {
        Protos.TaskStatus status = null;
        status = Protos.TaskStatus.newBuilder()
                .setTaskId(task.getTaskId())
                .setState(Protos.TaskState.TASK_STARTING).build();
        driver.sendStatusUpdate(status);
        Protos.Port clientPort;
        Protos.Port transportPort;
        if (task.hasDiscovery()) {
            List<Protos.Port> portsList = task.getDiscovery().getPorts().getPortsList();
            clientPort = portsList.get(Discovery.CLIENT_PORT_INDEX);
            transportPort = portsList.get(Discovery.TRANSPORT_PORT_INDEX);
        } else {
            LOGGER.error("The task must pass a DiscoveryInfoPacket");
            status = Protos.TaskStatus.newBuilder()
                    .setTaskId(task.getTaskId())
                    .setState(Protos.TaskState.TASK_ERROR).build();
            driver.sendStatusUpdate(status);
            return;
        }
        final Node node;
        try {
            node = launchElasticsearchNode(clientPort, transportPort);
        } catch (IOException e) {
            LOGGER.error(e);
            status = Protos.TaskStatus.newBuilder()
                    .setTaskId(task.getTaskId())
                    .setState(Protos.TaskState.TASK_FAILED).build();
            driver.sendStatusUpdate(status);
            return;
        }

        status = Protos.TaskStatus.newBuilder()
                .setTaskId(task.getTaskId())
                .setState(Protos.TaskState.TASK_RUNNING).build();
        driver.sendStatusUpdate(status);

        LOGGER.info("TASK_RUNNING");

        try {
            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                @Override
                public void run() {
                    Protos.TaskStatus taskStatus = Protos.TaskStatus.newBuilder()
                            .setTaskId(task.getTaskId())
                            .setState(Protos.TaskState.TASK_FINISHED).build();
                    driver.sendStatusUpdate(taskStatus);
                    node.close();
                    LOGGER.info("TASK_FINSHED");
                }
            }) {
            });
        } catch (Exception e) {
            status = Protos.TaskStatus.newBuilder()
                    .setTaskId(task.getTaskId())
                    .setState(Protos.TaskState.TASK_FAILED).build();
            driver.sendStatusUpdate(status);
        }
    }

    private static Node launchElasticsearchNode(Protos.Port clientPort, Protos.Port transportPort) throws IOException {
        FileSystemUtils.mkdirs(new File("plugins"));
        String url = String.format(Binaries.ES_CLOUD_MESOS_FILE_URL, System.getProperty("user.dir"));
        Environment environment = new Environment();
        PluginManager manager = new PluginManager(environment, url, PluginManager.OutputMode.VERBOSE, TimeValue.timeValueMinutes(5));
        manager.downloadAndExtract(Binaries.ES_CLOUD_MESOS_PLUGIN_NAME);

        LOGGER.info("Installed elasticsearch-cloud-mesos plugin");

        Settings settings = ImmutableSettings.settingsBuilder()
                .put("discovery.type", "auto")
                .put("cloud.enabled", "true")
                .put("foreground", "true")
                .put("master", "true")
                .put("data", "true")
                .put("script.disable_dynamic", "false")
                .put("logger.discovery", "debug")
                .put("logger.cloud.mesos", "debug").build();

        final Node node = NodeBuilder.nodeBuilder().settings(settings).build();
//        node.start();
//        Settings settings = ImmutableSettings.settingsBuilder()
//                .put("node.local", false)
//                .put("cluster.name", "mesos-elasticsearch")
//                .put("node.master", true)
//                .put("node.data", true)
//                .put("index.number_of_shards", 5)
//                .put("index.number_of_replicas", 1)
//                .put("discovery.zen.ping.multicast.enabled", false)
//                .put("discovery.zen.ping.unicast.hosts", "node2:9200")
//                .put("http.port", String.valueOf(clientPort.getNumber()))
//                .put("transport.tcp.port", String.valueOf(transportPort.getNumber()))
//                .build();
//        Node node = NodeBuilder.nodeBuilder().local(false).settings(settings).node();
//        cluster.name: mycluster
//        name.name: NODE1
//        node.master: true
//        node.data: true
//        index.number_of_shards: 5
//        index.number_of_replicas: 1
//        discovery.zen.ping.multicast.enabled: false
//        discovery.zen.ping.unicast.hosts: ["node2:9200"]
        return node;
    }

    @Override
    public void killTask(ExecutorDriver driver, Protos.TaskID taskId) {
        LOGGER.info("Kill task: " + taskId.getValue());
        Protos.TaskStatus status = Protos.TaskStatus.newBuilder()
                .setTaskId(taskId)
                .setState(Protos.TaskState.TASK_FAILED).build();
        driver.sendStatusUpdate(status);
    }

    @Override
    public void frameworkMessage(ExecutorDriver driver, byte[] data) {
        LOGGER.info("Framework message: " + Arrays.toString(data));
    }

    @Override
    public void shutdown(ExecutorDriver driver) {
        LOGGER.info("Shutting down framework...");
    }

    @Override
    public void error(ExecutorDriver driver, String message) {
        LOGGER.info("Error in executor: " + message);
    }

    public static void main(String[] args) throws Exception {
        MesosExecutorDriver driver = new MesosExecutorDriver(new ElasticsearchExecutor());
        System.exit(driver.run() == Protos.Status.DRIVER_STOPPED ? 0 : 1);
    }
}
