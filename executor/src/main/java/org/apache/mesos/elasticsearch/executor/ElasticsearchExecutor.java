package org.apache.mesos.elasticsearch.executor;

import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.common.Binaries;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.plugins.PluginManager;

import java.io.File;
import java.util.Arrays;
import java.util.logging.Logger;

/**
 * Executor for Elasticsearch.
 */
@SuppressWarnings("PMD.TooManyMethods")
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
        new Thread() {

            @Override
            public void interrupt() {
                Protos.TaskStatus status = Protos.TaskStatus.newBuilder()
                        .setTaskId(task.getTaskId())
                        .setState(Protos.TaskState.TASK_FINISHED).build();
                driver.sendStatusUpdate(status);
            }

            @Override
            public void setUncaughtExceptionHandler(UncaughtExceptionHandler uncaughtExceptionHandler) {
                Protos.TaskStatus status = Protos.TaskStatus.newBuilder()
                        .setTaskId(task.getTaskId())
                        .setState(Protos.TaskState.TASK_FAILED).build();
                driver.sendStatusUpdate(status);
            }

            @Override
            public void run() {
                try {
                    Protos.TaskStatus status = Protos.TaskStatus.newBuilder()
                            .setTaskId(task.getTaskId())
                            .setState(Protos.TaskState.TASK_RUNNING).build();
                    driver.sendStatusUpdate(status);

                    FileSystemUtils.mkdirs(new File("plugins"));
                    String url = String.format(Binaries.ES_CLOUD_MESOS_FILE_URL, System.getProperty("user.dir"));
                    Environment environment = new Environment();
                    PluginManager manager = new PluginManager(environment, url, PluginManager.OutputMode.VERBOSE, TimeValue.timeValueMinutes(5));
                    manager.downloadAndExtract(Binaries.ES_CLOUD_MESOS_PLUGIN_NAME);

                    LOGGER.info("Installed elasticsearch-cloud-mesos plugin");

                    System.setProperty("es.discovery.type", "mesos");
                    System.setProperty("es.cloud.enabled", "true");
                    System.setProperty("es.foreground", "true");
                    System.setProperty("es.logger.discovery", "DEBUG");

                    NodeBuilder.nodeBuilder().build().start();
                } catch (Exception e) {
                    Protos.TaskStatus status = Protos.TaskStatus.newBuilder()
                            .setTaskId(task.getTaskId())
                            .setState(Protos.TaskState.TASK_FAILED).build();
                    driver.sendStatusUpdate(status);
                }
            }
        }.start();
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
