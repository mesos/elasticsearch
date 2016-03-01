package org.apache.mesos.elasticsearch.scheduler.state;

import java.io.IOException;

/**
 * Uses zookeeper state to write a Protocol Buffer packet
 *
 * @param <T> The object to store. Must be serializable.
 */
public class ESState<T> {
    private final SerializableState state;
    private final String key;

    public ESState(SerializableState state, StatePath statePath, String key) {
        this.state = state;
        this.key = key;
        createKey(statePath, key);
    }

    public void set(T message) {
        try {
            state.set(key, message);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to write value for " + key + " to zookeeper", e);
        }
    }

    public T get() {
        try {
            return state.get(key);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to get value for " + key + " from zookeeper", e);
        }
    }

    private void createKey(StatePath statePath, String key) {
        try {
            if (!statePath.exists(key)) {
                statePath.mkdir(key);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Unable to create key " + key + " in zookeeper", e);
        }
    }

    public void destroy() {
        try {
            set(null);
            state.delete(key);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to delete " + key + " from zookeeper");
        }
    }
}
