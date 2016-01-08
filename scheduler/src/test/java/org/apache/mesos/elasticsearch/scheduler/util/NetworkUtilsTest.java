package org.apache.mesos.elasticsearch.scheduler.util;

import org.junit.Test;

import java.util.regex.Pattern;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 */
public class NetworkUtilsTest {

    private final NetworkUtils networkUtils = new NetworkUtils();

    private static final Pattern PATTERN = Pattern.compile(
            "^(([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.){3}([01]?\\d\\d?|2[0-4]\\d|25[0-5])$");

    public static boolean validate(final String ip) {
        return PATTERN.matcher(ip).matches();
    }

    @Test
    // Note: On OSX, when not connected to a network, it will return a IPv6 address, which will not validate properly.
    // Please connect to a network to obtain a IPv4 address.
    public void shouldProvideIPAddress() {
        int port = 1234;
        String string = networkUtils.addressToString(networkUtils.hostSocket(port), true);
        assertTrue(validate(string.replace("http://", "").replace(":" + port, "")));
    }


    @Test
    public void shouldProvideHostname() {
        int port = 1234;
        String string = networkUtils.addressToString(networkUtils.hostSocket(port), false);
        assertFalse(validate(string.replace("http://", "").replace(":" + port, "")));
    }
}