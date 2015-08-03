package org.apache.mesos.elasticsearch.scheduler.state;

import org.apache.mesos.elasticsearch.scheduler.state.zookeeper.ZooKeeperStateInterface;
import org.apache.mesos.state.Variable;

import java.io.*;

/**
 * Writes serializable data to zookeeper
 */
public class SerializableZookeeperState implements SerializableState {
    private ZooKeeperStateInterface zkState;

    public SerializableZookeeperState(ZooKeeperStateInterface zkState) {
        this.zkState = zkState;
    }

    /**
     * Get serializable object from store
     * null if none
     *
     * @return Object
     * @throws NotSerializableException
     */
    @SuppressWarnings({"unchecked", "REC_CATCH_EXCEPTION"})
    public <T> T get(String key) throws NotSerializableException{
        try {
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
        } catch (Exception e) {
            throw new NotSerializableException();
        }
    }

    /**
     * Set serializable object in store
     *
     * @throws NotSerializableException
     */
    @SuppressWarnings("REC_CATCH_EXCEPTION")
    public <T> void set(String key, T object) throws NotSerializableException {
        try {
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
        } catch (Exception e) {
            throw new NotSerializableException();
        }
    }
}
