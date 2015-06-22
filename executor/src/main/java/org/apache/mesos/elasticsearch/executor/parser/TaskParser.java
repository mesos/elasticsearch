package org.apache.mesos.elasticsearch.executor.parser;

import org.apache.mesos.Protos;

import java.security.InvalidAlgorithmParameterException;

/**
 * Parses TaskInfo packet
 * @param <T> is the return type of the item you are wishing to parse.
 */
public interface TaskParser<T> {
    T parse(Protos.TaskInfo taskInfo) throws InvalidAlgorithmParameterException;
}
