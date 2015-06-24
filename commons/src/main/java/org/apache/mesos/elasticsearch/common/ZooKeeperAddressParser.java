package org.apache.mesos.elasticsearch.common;

import java.security.InvalidParameterException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Ensures ZooKeeper addresses are formatted properly
 */
public class ZooKeeperAddressParser {
    private static final String userAndPass = "[^/@]+";
    private static final String hostAndPort = "[A-z0-9-.]+(?::\\d+)?";
    private static final String zkNode = "[^/]+";
    private static final String REGEX = "^zk://((?:" + userAndPass + "@)?(?:" + hostAndPort + "(?:," + hostAndPort + ")*))(/" + zkNode + "(?:/" + zkNode + ")*)*$";
    private static final String validZkUrl = "zk://host1:port1,host2:port2,.../path";
    private static final Pattern zkURLPattern = Pattern.compile(REGEX);

    public static Matcher validateZkUrl(final String zkUrl) {
        final Matcher matcher = zkURLPattern.matcher(zkUrl);

        if (!matcher.matches()) {
            throw new InvalidParameterException(String.format("Invalid zk url format: '%s' expected '%s'", zkUrl, validZkUrl));
        }
        return matcher;
    }
}
