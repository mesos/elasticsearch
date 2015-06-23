package org.apache.mesos.elasticsearch.executor.model;

import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.executor.parser.ParseZooKeeper;
import org.apache.mesos.elasticsearch.executor.parser.TaskParser;
import org.elasticsearch.common.settings.ImmutableSettings;

/**
 * Model representing ZooKeeper information
 */
public class ZooKeeperModel {
    public static final String ZOOKEEPER_ADDRESS_KEY = "sonian.elasticsearch.zookeeper.client.host";
    private final TaskParser<String> parser = new ParseZooKeeper();
    private final String address;

    public ZooKeeperModel(Protos.TaskInfo taskInfo) {
        address = parser.parse(taskInfo);
    }

    public ImmutableSettings.Builder getAddress() {
        return ImmutableSettings.settingsBuilder().put(ZOOKEEPER_ADDRESS_KEY, address);
    }
}
