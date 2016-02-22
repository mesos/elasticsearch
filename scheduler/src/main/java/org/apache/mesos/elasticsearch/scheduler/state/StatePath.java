package org.apache.mesos.elasticsearch.scheduler.state;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.security.InvalidParameterException;

/**
 * Path utilities
 */
public class StatePath {
    private static final Logger LOGGER = Logger.getLogger(StatePath.class);
    private SerializableState zkState;
    public StatePath(SerializableState zkState) {
        this.zkState = zkState;
    }

    /**
     * Creates the zNode if it does not exist. Will create parent directories.
     * @param key the zNode path
     */
    public void mkdir(String key) throws IOException {
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

    public Boolean exists(String key) throws IOException {
        Boolean exists = true;
        Object value = zkState.get(key);
        if (value == null) {
            exists = false;
        }
        return exists;
    }

    public void rm(String key) throws IOException {
        key = key.replace(" ", "");
        zkState.delete(key);
    }
}
