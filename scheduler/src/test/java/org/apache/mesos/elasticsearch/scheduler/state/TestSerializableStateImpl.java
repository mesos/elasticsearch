package org.apache.mesos.elasticsearch.scheduler.state;

import java.io.IOException;
import java.io.NotSerializableException;
import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.Map;

/**
 * Dummy storage class to replace zookeeper.
 */
@SuppressWarnings("unchecked")
public class TestSerializableStateImpl implements SerializableState {
    Map<String, Object> map = new HashMap<>();

    @Override
    public <T> T get(String key) throws IOException {
        return (T) map.getOrDefault(key, null);
    }

    @Override
    public <T> void set(String key, T object) throws IOException {
        if (key.endsWith("/") && !key.equals("/")) {
            throw new NotSerializableException("Trailing slashes are not allowed");
        }
        map.put(key, object);
    }

    @Override
    public void delete(String key) throws IOException {
        if (map.remove(key) == null) {
            throw new InvalidParameterException("Unable to delete key.");
        }
    }
}