package org.apache.mesos.elasticsearch.common.util;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.exec.environment.EnvironmentUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.*;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Map;

/**
 * Utilities to help with networking
 */
@SuppressWarnings("PMD.AvoidUsingHardCodedIP")
public class NetworkUtils {
    private static final Logger LOG = Logger.getLogger(NetworkUtils.class);
    public static final String DOCKER_MACHINE_IP = "docker-machine ip";
    public static final String LOCALHOST = "127.0.0.1";
    public static final String DOCKER_MACHINE_NAME = "DOCKER_MACHINE_NAME";

    public static InetAddress hostAddress() {
        try {
            return InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            LOG.error("", e);
            throw new RuntimeException("Unable to bind to local host.");
        }
    }

    public static InetSocketAddress hostSocket(int port) {
        return new InetSocketAddress(hostAddress(), port);
    }

    public static String addressToString(InetSocketAddress address, Boolean useIpAddress) {
        if (useIpAddress) {
            return "http://" + address.getAddress().getHostAddress() + ":" + address.getPort();
        } else {
            return "http://" + address.getAddress().getHostName() + ":" + address.getPort();
        }
    }

    public static String getDockerMachineName(Map<String, String> environment) {
        String envVar = DOCKER_MACHINE_NAME;
        String dockerMachineName = environment.getOrDefault(envVar, "");
        if (dockerMachineName == null || dockerMachineName.isEmpty()) {
            LOG.debug("The environmental variable DOCKER_MACHINE_NAME was not found. Using docker0 address.");
        }
        return dockerMachineName;
    }

    public static String getDockerHostIpAddress(Map<String, String> environment) {
        String ipAddress = LOCALHOST; // Default of localhost
        String dockerMachineName = getDockerMachineName(environment);

        if (!dockerMachineName.isEmpty()) {
            LOG.debug("Docker machine name = " + dockerMachineName);
            CommandLine commandline = CommandLine.parse(DOCKER_MACHINE_IP);
            commandline.addArgument(dockerMachineName);
            LOG.debug("Running exec: " + commandline.toString());
            try {
                ipAddress = StringUtils.strip(runCommand(commandline));
            } catch (IOException e) {
                LOG.error("Unable to run exec command to find ip address.", e);
            }
        } else {
            ipAddress = getDocker0AdapterIPAddress();
        }
        LOG.debug("Returned IP address: " + ipAddress);
        return ipAddress;
    }

    public static Map<String, String> getEnvironment() {
        Map<String, String> env = Collections.emptyMap();
        try {
            env = EnvironmentUtils.getProcEnvironment();
        } catch (IOException e) {
            LOG.error("Unable to get environmental variables", e);
        }
        return env;
    }

    public static String runCommand(CommandLine commandline) throws IOException {
        DefaultExecutor exec = new DefaultExecutor();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PumpStreamHandler streamHandler = new PumpStreamHandler(outputStream);
        exec.setStreamHandler(streamHandler);
        exec.execute(commandline);
        return outputStream.toString(Charset.defaultCharset().name());
    }

    public static String getDocker0AdapterIPAddress() {
        InetAddress docker0 = getLocalAddress("docker0");
        if (docker0 == null) {
            LOG.error("Could not get address for docker0");
            return LOCALHOST;
        } else {
            return docker0.getHostAddress();
        }
    }

    private static InetAddress getLocalAddress(String adaptorName){
        try {
            Enumeration<NetworkInterface> b = NetworkInterface.getNetworkInterfaces();
            while (b.hasMoreElements()) {
                NetworkInterface networkInterface = b.nextElement();
                if (networkInterface.getName().equals(adaptorName)) {
                    for (InterfaceAddress f : networkInterface.getInterfaceAddresses()) {
                        if (f.getAddress().isSiteLocalAddress()) {
                            return f.getAddress();
                        }
                    }
                }
            }
        } catch (SocketException e) {
            e.printStackTrace();
        }
        return null;
    }
}