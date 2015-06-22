package org.apache.mesos.elasticsearch.executor.model;

import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.executor.parser.ParseZooKeeper;
import org.apache.mesos.elasticsearch.executor.parser.TaskParser;

import java.security.InvalidAlgorithmParameterException;

/**
 * Model representing ZooKeeper information
 */
public class ZooKeeperModel {
    private final TaskParser<String> parser = new ParseZooKeeper();
    private final String address;

    public ZooKeeperModel(Protos.TaskInfo taskInfo) throws InvalidAlgorithmParameterException {
        address = parser.parse(taskInfo);
    }

    public String getAddress() {
        return address;
    }
}
