package org.apache.mesos.elasticsearch.common;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Ensures ZooKeeper addresses are formatted properly
 */
public class ZooKeeperAddressParser {
    private static final String userAndPass = "[^/@]+";
    private static final String hostAndPort = "([A-z0-9-.]+)(?::)([0-9]+)";
    private static final String zkNode = "[^/]+";
    public static final String ADDRESS_REGEX = "^(" + userAndPass + "@)?" + hostAndPort + "(/" + zkNode + ")?$";
    private static final String ZK_PREFIX = "zk://";
    private static final String ZK_PREFIX_REGEX = "^" + ZK_PREFIX + ".*";

    public static List<ZooKeeperAddress> validateZkUrl(final String zkUrl) {
        final List<ZooKeeperAddress> zkList = new ArrayList<>();

        // Ensure that string is prefixed with "zk://"
        Matcher matcher = Pattern.compile(ZK_PREFIX_REGEX).matcher(zkUrl);
        if (!matcher.matches()) {
            throw new ZooKeeperAddressException(zkUrl);
        }

        // Strip zk prefix and spaces
        String zkStripped = zkUrl.replace(ZK_PREFIX, "").replace(" ", "");

        // Split address by commas
        String[] split = zkStripped.split(",");

        // Validate and add each split
        for (String s : split) {
            zkList.add(new ZooKeeperAddress(s));
        }

        // Return list of zk addresses
        return zkList;
    }
}
