package org.apache.mesos.elasticsearch.scheduler.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

/**
 * Utilities to help with networking
 */
public class NetworkUtils {
    private static final Logger LOG = LoggerFactory.getLogger(NetworkUtils.class);

    public InetAddress hostAddress() {
        try {
            return InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            LOG.error("", e);
            throw new RuntimeException("Unable to bind to local host.");
        }
    }

    public InetSocketAddress hostSocket(int port) {
        return new InetSocketAddress(hostAddress(), port);
    }

    public String addressToString(InetSocketAddress address, Boolean useIpAddress) {
        if (useIpAddress) {
            return "http://" + address.getAddress().getHostAddress() + ":" + address.getPort();
        } else {
            return "http://" + address.getAddress().getHostName() + ":" + address.getPort();
        }
    }
}