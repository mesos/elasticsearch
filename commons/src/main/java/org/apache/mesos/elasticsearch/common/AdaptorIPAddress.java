package org.apache.mesos.elasticsearch.common;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Obtains the first IP address from an adaptor
 */
public class AdaptorIPAddress {
    private static final org.apache.log4j.Logger LOGGER = org.apache.log4j.Logger.getLogger(AdaptorIPAddress.class);
    /**
     * @return an InetAddress for the adaptor eth0 or en0 (so unit tests work on mac)
     * @throws SocketException if adaptor doesn't exist or it doesn't have an IP Address
     */
    public static InetAddress eth0() throws SocketException {
        Enumeration<NetworkInterface> nets = NetworkInterface.getNetworkInterfaces();
        ArrayList<NetworkInterface> netList = Collections.list(nets);
        LOGGER.debug(netList.stream().map(NetworkInterface::getName).collect(Collectors.joining(", ")));
        NetworkInterface eth0 = netList.stream().filter(s -> s.getName().matches("eth0|en0")).findFirst().get();
        ArrayList<InetAddress> inetAddresses = Collections.list(eth0.getInetAddresses());
        Optional<InetAddress> address = inetAddresses.stream().filter(inetAddress -> inetAddress.getHostAddress().matches("^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$")).findFirst();
        LOGGER.debug(eth0.getDisplayName() + ": " + address.get().getHostAddress());
        return address.get();
    }
}
