package org.apache.mesos.elasticsearch.executor;

import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;

/**
 * Executor for Elasticsearch.
 */
public class ElasticsearchExecutor implements Executor {

    @Override
    public void registered(ExecutorDriver driver, Protos.ExecutorInfo executorInfo, Protos.FrameworkInfo frameworkInfo, Protos.SlaveInfo slaveInfo) {

    }

    @Override
    public void reregistered(ExecutorDriver driver, Protos.SlaveInfo slaveInfo) {

    }

    @Override
    public void disconnected(ExecutorDriver driver) {

    }

    @Override
    public void launchTask(ExecutorDriver driver, Protos.TaskInfo task) {

    }

    @Override
    public void killTask(ExecutorDriver driver, Protos.TaskID taskId) {

    }

    @Override
    public void frameworkMessage(ExecutorDriver driver, byte[] data) {

    }

    @Override
    public void shutdown(ExecutorDriver driver) {

    }

    @Override
    public void error(ExecutorDriver driver, String message) {

    }
}
