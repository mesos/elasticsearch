package org.apache.mesos.elasticsearch.common;

import java.security.InvalidParameterException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Ensures ZooKeeper addresses are formatted properly
 */
public class ZooKeeperAddressParser {
    private static final String USER_AND_PASS = "[^/@]+";
    private static final String HOST_AND_PORT = "[A-z0-9-.]+(?::\\d+)?";
    private static final String ZKNODE = "[^/]+";
    private static final String REGEX = "^zk://((?:" + USER_AND_PASS + "@)?(?:" + HOST_AND_PORT + "(?:," + HOST_AND_PORT + ")*))(/" + ZKNODE + "(?:/" + ZKNODE + ")*)*$";
    private static final String VALID_ZK_URL = "zk://host1:port1,host2:port2,.../path";
    private static final Pattern ZK_URL_PATTERN = Pattern.compile(REGEX);

    private ZooKeeperAddressParser() {

    }

    public static Matcher validateZkUrl(final String zkUrl) {
        final Matcher matcher = ZK_URL_PATTERN.matcher(zkUrl);

        if (!matcher.matches()) {
            throw new InvalidParameterException(String.format("Invalid zk url format: '%s' expected '%s'", zkUrl, VALID_ZK_URL));
        }
        return matcher;
    }
}
