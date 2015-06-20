package org.apache.mesos.elasticsearch.scheduler;

import com.google.protobuf.InvalidProtocolBufferException;
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
    private static final String FRAMEWORKID_KEY = "frameworkId";
    private ZooKeeperStateInterface zkState;

    public State(ZooKeeperStateInterface zkState) {
        this.zkState = zkState;
    }

    /**
     * return null if no frameworkId found
     *
     * @throws ExecutionException
     * @throws InterruptedException
     * @throws InvalidProtocolBufferException
     */
    public FrameworkID getFrameworkID() throws InterruptedException, ExecutionException, InvalidProtocolBufferException {
        byte[] existingFrameworkId = zkState.fetch(FRAMEWORKID_KEY).get().value();
        if (existingFrameworkId.length > 0) {
            return FrameworkID.parseFrom(existingFrameworkId);
        } else {
            return null;
        }
    }

    public void setFrameworkId(FrameworkID frameworkId) throws InterruptedException, ExecutionException {
        Variable value = zkState.fetch(FRAMEWORKID_KEY).get();
        value = value.mutate(frameworkId.toByteArray());
        zkState.store(value).get();
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
    private <T extends Object> T get(String key) throws InterruptedException, ExecutionException, IOException, ClassNotFoundException {
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
    private <T extends Object> void set(String key, T object) throws InterruptedException, ExecutionException, IOException {
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
