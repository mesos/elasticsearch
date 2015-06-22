package org.apache.mesos.elasticsearch.executor.parser;

import org.apache.mesos.Protos;

import java.security.InvalidAlgorithmParameterException;
import java.util.List;

/**
 * Parses ports from task info
 */
public class ParsePorts implements TaskParser<List<Protos.Port>> {
    @Override
    public List<Protos.Port> parse(Protos.TaskInfo taskInfo) throws InvalidAlgorithmParameterException {
        if (taskInfo.hasDiscovery()) {
            return taskInfo.getDiscovery().getPorts().getPortsList();
        } else {
            throw new InvalidAlgorithmParameterException("The task must pass a DiscoveryInfoPacket");
        }
    }
}
