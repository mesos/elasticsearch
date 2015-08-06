package org.apache.mesos.elasticsearch.scheduler.state;

import java.io.IOException;

/**
 * Represents a serializable interface
 */
public interface SerializableState {
    <T> T get(String key) throws IOException;
    <T> void set(String key, T object) throws IOException;
    void delete(String key) throws IOException;
}
