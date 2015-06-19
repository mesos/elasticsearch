package org.apache.mesos.elasticsearch.common;

import org.apache.log4j.Logger;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Utility for resolving hostnames. Needed for system test.
 */
public class HostResolver {

    public static final Logger LOGGER = Logger.getLogger(HostResolver.class);

    public static InetAddress resolve(String host) {
        try {
            return InetAddress.getByName(host);
        } catch (UnknownHostException e) {
            LOGGER.error("Could not resolve IP address for host '" + host + "'");
            return null;
        }
    }

}
