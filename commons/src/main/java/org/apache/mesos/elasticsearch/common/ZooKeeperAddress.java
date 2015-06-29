package org.apache.mesos.elasticsearch.common;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Model representing ZooKeeper addresses
 */
public class ZooKeeperAddress {
    private String user;
    private String password;
    private String address;
    private String port;
    private String zkNode;

    /**
     * Represents a single zookeeper address.
     *
     * @param address Must be in the format [user:password@]host[:port] where [] are optional.
     */
    public ZooKeeperAddress(String address) {
        Matcher matcher = Pattern.compile(ZooKeeperAddressParser.ADDRESS_REGEX).matcher(address);
        if (!matcher.matches()) {
            throw new ZooKeeperAddressException(address);
        }
        setUser(matcher.group(0));
        setPassword(matcher.group(1));
        setAddress(matcher.group(2));
        setPort(matcher.group(3));
        setZkNode(matcher.group(4));
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = "";
        if (user != null) {
            this.user = user;
        }
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = "";
        if (password != null) {
            this.password = password;
        }
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = "";
        if (address != null) {
            this.address = address;
        }
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = "";
        if (port != null) {
            this.port = port;
        }
    }

    public String getZkNode() {
        return zkNode;
    }

    public void setZkNode(String zkNode) {
        this.zkNode = "";
        if (zkNode != null) {
            this.zkNode = zkNode;
        }
    }
}


