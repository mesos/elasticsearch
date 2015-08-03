package org.apache.mesos.elasticsearch.scheduler.state;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.log4j.Logger;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.elasticsearch.scheduler.state.zookeeper.ZooKeeperStateInterface;
import org.apache.mesos.state.Variable;

import java.io.*;
import java.security.InvalidParameterException;
import java.util.concurrent.ExecutionException;

/**
 * DCOS certification requirement 02
 * This allows the scheduler to persist the key/value pairs to zookeeper.
 */
public class State {
    private static final Logger LOGGER = Logger.getLogger(State.class);
    private static final String FRAMEWORKID_KEY = "frameworkId";
    private SerializableState zkState;
    public State(SerializableState zkState) {
        this.zkState = zkState;
    }

    /**
     * Return empty if no frameworkId found.
     */
    public FrameworkID getFrameworkID() throws NotSerializableException {
        FrameworkID id = zkState.get(FRAMEWORKID_KEY);
        if (id == null) {
            id = FrameworkID.newBuilder().setValue("").build();
        }
        return id;
    }

    public void setFrameworkId(FrameworkID frameworkId) throws NotSerializableException {
        setAndCreateParents(FRAMEWORKID_KEY, frameworkId);
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
            if (!s.isEmpty()) {
                if (!exists(builder.toString())) {
                    zkState.set(builder.toString(), null);
                }
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
