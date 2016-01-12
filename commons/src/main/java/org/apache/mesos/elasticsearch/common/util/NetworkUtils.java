package org.apache.mesos.elasticsearch.common.util;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.exec.environment.EnvironmentUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.Collections;
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
        String docker_machine_name = environment.getOrDefault(envVar, "");
        if (docker_machine_name == null || docker_machine_name.isEmpty()) {
            LOG.debug("The environmental variable DOCKER_MACHINE_NAME was not found. Using localhost.");
        }
        return docker_machine_name;
    }

    public static String getDockerHostIpAddress(Map<String, String> environment) {
        String ipAddress = LOCALHOST; // Default of localhost
        String docker_machine_name = getDockerMachineName(environment);

        if (!docker_machine_name.isEmpty()) {
            LOG.debug("Docker machine name = " + docker_machine_name);
            CommandLine commandline = CommandLine.parse(DOCKER_MACHINE_IP);
            commandline.addArgument(docker_machine_name);
            LOG.debug("Running exec: " + commandline.toString());
            try {
                ipAddress = StringUtils.strip(runCommand(commandline));
            } catch (IOException e) {
                LOG.error("Unable to run exec command to find ip address.", e);
            }
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
}