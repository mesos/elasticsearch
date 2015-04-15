package org.apache.mesos.elasticsearch.executor;

import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.common.Binaries;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.plugins.PluginManager;

import java.util.Arrays;
import java.util.logging.Logger;

/**
 * Executor for Elasticsearch.
 */
public class ElasticsearchExecutor implements Executor {

    public static final Logger LOGGER = Logger.getLogger(ElasticsearchExecutor.class.toString());

    public static void main(String[] args) {
        System.out.println("Started Executor...");

        MesosExecutorDriver driver = new MesosExecutorDriver(new ElasticsearchExecutor());
        Protos.Status status = driver.run();
        if (status.equals(Protos.Status.DRIVER_STOPPED)) {
            System.exit(0);
        } else {
            System.exit(1);
        }
    }

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
        String[] pluginArgs = new String[]{
                "--url", String.format(Binaries.ES_CLOUD_MESOS_FILE_URL, System.getProperty("user.dir")),
                "--install", Binaries.ES_CLOUD_MESOS_PLUGIN_NAME,
                "--verbose",
                "--timeout", "20000"
        };
        PluginManager.main(pluginArgs);

        LOGGER.info("Installed elasticsearch-cloud-mesos plugin");

        Protos.TaskStatus.Builder status = Protos.TaskStatus.newBuilder().setTaskId(task.getTaskId());
        status.setState(Protos.TaskState.TASK_RUNNING);
        driver.sendStatusUpdate(status.build());

        try {
            ImmutableSettings.Builder settings = NodeBuilder.nodeBuilder().settings();
            settings.put("--discovery.type", "cloud-mesos");
            settings.put("--cloud.enabled", "true");
            settings.put("--logger.discovery", "DEBUG");
            settings.put("--foreground", "true");

            Node node = NodeBuilder.nodeBuilder().clusterName("elasticsearch").settings(settings).build();
            node.start();

            status.setState(Protos.TaskState.TASK_FINISHED);
        } catch (Exception e) {
            status.setState(Protos.TaskState.TASK_FAILED);
        }

        driver.sendStatusUpdate(status.build());
    }

    @Override
    public void killTask(ExecutorDriver driver, Protos.TaskID taskId) {
        LOGGER.info("Kill task: " + taskId.getValue());
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
}
