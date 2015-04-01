package org.apache.mesos.elasticsearch.executor;

import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.plugins.PluginManager;

import java.io.IOException;
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
    public void launchTask(ExecutorDriver driver, Protos.TaskInfo task) {
        Node node = NodeBuilder.nodeBuilder().clusterName("elasticsearch").node();
        Environment environment = new Environment(node.settings());
        String url = "file://es-cloud-mesos-0.0.1-SNAPSHOT.zip";
        PluginManager.OutputMode outputMode = PluginManager.OutputMode.VERBOSE;
        TimeValue timeValue = new TimeValue(20000);
        PluginManager manager = new PluginManager(environment, url, outputMode, timeValue);
        try {
            manager.downloadAndExtract("cloud-mesos");
            LOGGER.info("Installed elasticsearch-cloud-mesos plugin");
        } catch (IOException e) {
            // TODO: Check why plugin could not be installed
            e.printStackTrace();
            LOGGER.severe("Could not install elasticsearch-cloud-mesos plugin");
        }
    }

    @Override
    public void killTask(ExecutorDriver driver, Protos.TaskID taskId) {

    }

    @Override
    public void frameworkMessage(ExecutorDriver driver, byte[] data) {
        LOGGER.info("Framework message " + Arrays.toString(data));
    }

    @Override
    public void shutdown(ExecutorDriver driver) {

    }

    @Override
    public void error(ExecutorDriver driver, String message) {

    }
}
