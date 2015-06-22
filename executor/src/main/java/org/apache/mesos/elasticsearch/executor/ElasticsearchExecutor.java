package org.apache.mesos.elasticsearch.executor;

import org.apache.log4j.Logger;
import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.common.Discovery;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.io.IOException;
import java.util.*;

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

        LOGGER.info("Starting executor with a TaskInfo of:");
        LOGGER.info(task.toString());

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

        String zkNode;
        int nargs = task.getExecutor().getCommand().getArgumentsCount();
        LOGGER.info("Using arguments [" + nargs + "]: " + task.getExecutor().getCommand().getArgumentsList().toString());
        if (nargs > 0 && nargs % 2 == 0) {
            Map<String, String> argMap = new HashMap<>(1);
            Iterator<String> itr = task.getExecutor().getCommand().getArgumentsList().iterator();
            while (itr.hasNext()) {
                argMap.put(itr.next(), itr.next());
            }
            zkNode = argMap.get("-zk");
        } else {
            LOGGER.error("The task must pass a ZooKeeper address argument using -zk.");
            status = Protos.TaskStatus.newBuilder()
                    .setTaskId(task.getTaskId())
                    .setState(Protos.TaskState.TASK_ERROR).build();
            driver.sendStatusUpdate(status);
            return;
        }
        final Node node;
        try {
            node = launchElasticsearchNode(zkNode, clientPort, transportPort);
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

    private Node launchElasticsearchNode(String zkNode, Protos.Port clientPort, Protos.Port transportPort) throws IOException {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("node.local", false)
                .put("cluster.name", "mesos-elasticsearch")
                .put("node.master", true)
                .put("node.data", true)
                .put("index.number_of_shards", 5)
                .put("index.number_of_replicas", 1)
                .put("http.port", String.valueOf(clientPort.getNumber()))
                .put("transport.tcp.port", String.valueOf(transportPort.getNumber()))
                .put("discovery.type", "com.sonian.elasticsearch.zookeeper.discovery.ZooKeeperDiscoveryModule")
                .put("sonian.elasticsearch.zookeeper.settings.enabled", true)
                .put("sonian.elasticsearch.zookeeper.client.host", zkNode)
                .put("sonian.elasticsearch.zookeeper.discovery.state_publishing.enabled", true)
                .build();
        Node node = NodeBuilder.nodeBuilder().local(false).settings(settings).node();
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
}
