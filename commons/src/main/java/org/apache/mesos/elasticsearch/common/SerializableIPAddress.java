package org.apache.mesos.elasticsearch.common;

import org.apache.commons.lang.builder.HashCodeBuilder;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * IP address to be sent over a mesos framework message
 */
public class SerializableIPAddress implements Serializable {
    public InetAddress getAddress() {
        return address;
    }

    private final InetAddress address;

    public SerializableIPAddress(InetAddress address) {
        this.address = address;
    }

    public byte[] toBytes() {
        return address.getAddress();
    }

    public static SerializableIPAddress fromBytes(byte[] data) throws UnknownHostException {
        return new SerializableIPAddress(InetAddress.getByAddress(data));
    }

    @Override
    public String toString() {
        return address.getHostAddress();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37).
                append(address).
                toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }

        SerializableIPAddress inetAddress = (SerializableIPAddress) obj;
        return this.getAddress().equals(inetAddress.getAddress());
    }
}
