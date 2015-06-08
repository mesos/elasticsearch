package org.apache.mesos.elasticsearch.scheduler;

import org.apache.mesos.state.Variable;

import java.util.concurrent.Future;

/**
 * A wrapper class to decouple ZooKeeper from the ES project.
 */
public interface ZooKeeperStateInterface {
    Future<Variable> fetch(final String name);
    Future<Variable> store(Variable variable);
}
