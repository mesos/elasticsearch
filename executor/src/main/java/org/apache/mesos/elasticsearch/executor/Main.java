package org.apache.mesos.elasticsearch.executor;

import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos;

/**
 * Application which starts the Elasticsearch executor
 */
public class Main {
    public static void main(String[] args) throws Exception {
        MesosExecutorDriver driver = new MesosExecutorDriver(new ElasticsearchExecutor());
        System.exit(driver.run() == Protos.Status.DRIVER_STOPPED ? 0 : 1);
    }
}
