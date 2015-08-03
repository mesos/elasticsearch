package org.apache.mesos.elasticsearch.scheduler.state;

import org.apache.log4j.Logger;

import java.io.NotSerializableException;
import java.security.InvalidParameterException;

/**
 * DCOS certification requirement 02
 * This allows the scheduler to persist the key/value pairs to zookeeper.
 */
public class State {
    private static final Logger LOGGER = Logger.getLogger(State.class);
    private final FrameworkState frameworkState = new FrameworkState(this);
    private SerializableState zkState;
    public State(SerializableState zkState) {
        this.zkState = zkState;
    }

    public <T> void setAndCreateParents(String key, T object) throws NotSerializableException {
        mkdir(key);
        zkState.set(key, object);
    }

    public <T> T get(String key) throws IllegalStateException {
        try {
            return zkState.get(key);
        } catch (NotSerializableException e) {
            throw new IllegalStateException("Unable to get key: " + key, e);
        }
    }

    /**
     * Creates the zNode if it does not exist
     * @param key the zNode path
     */
    public void mkdir(String key) throws NotSerializableException {
        key = key.replace(" ", "");
        if (key.endsWith("/") && !key.equals("/")) {
            throw new InvalidParameterException("Trailing slash not allowed in zookeeper path");
        }
        String[] split = key.split("/");
        StringBuilder builder = new StringBuilder();
        for (String s : split) {
            builder.append(s);
            if (!s.isEmpty() && !exists(builder.toString())) {
                zkState.set(builder.toString(), null);
            }
            builder.append("/");
        }
    }

    public Boolean exists(String key) throws NotSerializableException {
        Boolean exists = true;
        Object value = zkState.get(key);
        if (value == null) {
            exists = false;
        }
        return exists;
    }
}
