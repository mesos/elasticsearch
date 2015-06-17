package org.apache.mesos.elasticsearch.executor;

import org.apache.log4j.Logger;
import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.common.Binaries;
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
        LOGGER.info("driver");
        LOGGER.info(driver);
        LOGGER.info("task");
        LOGGER.info(task);


        Protos.TaskStatus status = Protos.TaskStatus.newBuilder()
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

    private static Node launchElasticsearchNode() throws IOException {
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
        node.start();
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
