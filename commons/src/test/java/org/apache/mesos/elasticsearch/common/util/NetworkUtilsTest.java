package org.apache.mesos.elasticsearch.common.util;

import org.apache.commons.validator.routines.InetAddressValidator;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;

import static org.junit.Assert.*;

/**
 */
public class NetworkUtilsTest {

    public static boolean validate(final String ip) {
        return InetAddressValidator.getInstance().isValid(ip);
    }

    @Test
    // Note: On OSX, when not connected to a network, it will return a IPv6 address, which will not validate properly.
    // Please connect to a network to obtain a IPv4 address.
    public void shouldProvideIPAddress() {
        int port = 1234;
        String string = NetworkUtils.addressToString(NetworkUtils.hostSocket(port), true);
        assertTrue(validate(string.replace("http://", "").replace(":" + port, "")));
    }


    @Test
    public void shouldProvideHostname() {
        int port = 1234;
        String string = NetworkUtils.addressToString(NetworkUtils.hostSocket(port), false);
        assertFalse(validate(string.replace("http://", "").replace(":" + port, "")));
    }

    @Test
    public void shouldGetDockerIPAddress() throws IOException {
        // Should always be either a valid IP or 127.0.0.1
        assertTrue(validate(NetworkUtils.getDockerHostIpAddress(NetworkUtils.getEnvironment())));
    }

    @Test
    public void shouldReturnLocahostWhenNoEnvVar() {
        assertEquals(NetworkUtils.LOCALHOST, NetworkUtils.getDockerHostIpAddress(Collections.emptyMap()));
    }

    @Test
    public void shouldReturnDockerMachineNameWhenIncluded() {
        HashMap<String, String> map = new HashMap<>();
        String dev = "dev";
        map.put(NetworkUtils.DOCKER_MACHINE_NAME, dev);
        assertEquals(dev, NetworkUtils.getDockerMachineName(map));
    }
}