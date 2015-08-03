package org.apache.mesos.elasticsearch.scheduler.state;

import org.apache.log4j.Logger;

import java.io.NotSerializableException;
import java.security.InvalidParameterException;

/**
 * DCOS certification requirement 02
 * This allows the scheduler to persist the key/value pairs to zookeeper.
 */
public class StatePath {
    private static final Logger LOGGER = Logger.getLogger(StatePath.class);
    private SerializableState zkState;
    public StatePath(SerializableState zkState) {
        this.zkState = zkState;
    }

    public <T> void setAndCreateParents(String key, T object) throws NotSerializableException {
        mkdir(key);
        zkState.set(key, object);
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
