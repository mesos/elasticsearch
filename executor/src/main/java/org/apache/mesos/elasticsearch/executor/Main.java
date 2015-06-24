package org.apache.mesos.elasticsearch.executor;

import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.executor.elasticsearch.ElasticsearchLauncher;
import org.apache.mesos.elasticsearch.executor.elasticsearch.ElasticsearchSettings;
import org.apache.mesos.elasticsearch.executor.elasticsearch.Launcher;
import org.apache.mesos.elasticsearch.executor.mesos.ElasticsearchExecutor;
import org.apache.mesos.elasticsearch.executor.mesos.TaskStatus;

/**
 * Application which starts the Elasticsearch executor
 */
public class Main {
    public static void main(String[] args) throws Exception {
        Launcher launcher = new ElasticsearchLauncher(new ElasticsearchSettings().defaultSettings());
        MesosExecutorDriver driver = new MesosExecutorDriver(new ElasticsearchExecutor(launcher, new TaskStatus()));
        System.exit(driver.run() == Protos.Status.DRIVER_STOPPED ? 0 : 1);
    }
}
