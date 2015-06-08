package org.apache.mesos.elasticsearch.scheduler;

import org.apache.mesos.state.Variable;
import org.apache.mesos.state.ZooKeeperState;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of ZooKeeperState
 */
public class ZooKeeperStateInterfaceImpl implements ZooKeeperStateInterface {
    public static final long ZK_TIMEOUT = 20000L;
    public static final String CLUSTER_NAME = "/mesos-ha";
    public static final String FRAMEWORK_NAME = "/elasticsearc-mesos";

    private final ZooKeeperState state;

    public ZooKeeperStateInterfaceImpl(String zkNode) {
        state = new ZooKeeperState(
                zkNode,
                ZK_TIMEOUT,
                TimeUnit.MILLISECONDS,
                FRAMEWORK_NAME + CLUSTER_NAME);
    }
    @Override
    public Future<Variable> fetch(String name) {
        return state.fetch(name);
    }

    @Override
    public Future<Variable> store(Variable variable) {
        return state.store(variable);
    }
}
