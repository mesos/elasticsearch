package org.apache.mesos.elasticsearch.scheduler;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.log4j.Logger;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.state.Variable;

import java.io.*;
import java.util.concurrent.ExecutionException;

/**
 * DCOS certification requirement 02
 * This allows the scheduler to persist the key/value pairs to zookeeper.
 *
 * Todo:
 * In the future, we can also persist other state information to be more resilient.
 */
public class State {

    public static final Logger LOGGER = Logger.getLogger(State.class);

    private static final String FRAMEWORKID_KEY = "frameworkId";

    private ZooKeeperStateInterface zkState;

    public State(ZooKeeperStateInterface zkState) {
        this.zkState = zkState;
    }

    /**
     * Return null if no frameworkId found.
     */
    public FrameworkID getFrameworkID() {
        try {
            byte[] existingFrameworkId = zkState.fetch(FRAMEWORKID_KEY).get().value();
            if (existingFrameworkId.length > 0) {
                return FrameworkID.parseFrom(existingFrameworkId);
            } else {
                return null;
            }
        } catch (InterruptedException | ExecutionException | InvalidProtocolBufferException e) {
            LOGGER.error("Could not retrieve framework ID", e);
            throw new RuntimeException("Could not retrieve framework ID", e);
        }
    }

    public void setFrameworkId(FrameworkID frameworkId) {
        try {
           Variable value = zkState.fetch(FRAMEWORKID_KEY).get();
           value = value.mutate(frameworkId.toByteArray());
           zkState.store(value).get();
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.error("Could not retrieve framework ID", e);
            throw new RuntimeException("Could not set framework ID", e);
        }
    }

    /**
     * Get serializable object from store
     * null if none
     *
     * @return Object
     * @throws ExecutionException
     * @throws InterruptedException
     * @throws IOException
     * @throws ClassNotFoundException
     */
    @SuppressWarnings("unchecked")
    private <T> T get(String key) throws InterruptedException, ExecutionException, IOException, ClassNotFoundException {
        byte[] existingNodes = zkState.fetch(key).get().value();
        if (existingNodes.length > 0) {
            ByteArrayInputStream bis = new ByteArrayInputStream(existingNodes);
            ObjectInputStream in = null;
            try {
                in = new ObjectInputStream(bis);
                return (T) in.readObject();
            } finally {
                try {
                    bis.close();
                } finally {
                    if (in != null) {
                        in.close();
                    }
                }
            }
        } else {
            return null;
        }
    }

    /**
     * Set serializable object in store
     *
     * @throws ExecutionException
     * @throws InterruptedException
     * @throws IOException
     */
    private <T> void set(String key, T object) throws InterruptedException, ExecutionException, IOException {
        Variable value = zkState.fetch(key).get();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream out = null;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(object);
            value = value.mutate(bos.toByteArray());
            zkState.store(value).get();
        } finally {
            try {
                if (out != null) {
                    out.close();
                }
            } finally {
                bos.close();
            }
        }
    }
}
