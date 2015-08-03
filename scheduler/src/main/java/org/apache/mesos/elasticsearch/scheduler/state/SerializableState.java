package org.apache.mesos.elasticsearch.scheduler.state;

import java.io.NotSerializableException;

/**
 * Represents a serializable interface
 */
public interface SerializableState {
    <T> T get(String key) throws NotSerializableException;
    <T> void set(String key, T object) throws NotSerializableException;
}
