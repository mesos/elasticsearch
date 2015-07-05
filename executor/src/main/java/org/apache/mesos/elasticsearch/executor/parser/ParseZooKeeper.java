package org.apache.mesos.elasticsearch.executor.parser;

import org.apache.log4j.Logger;
import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.common.zookeeper.ZooKeeper;
import org.apache.mesos.elasticsearch.common.zookeeper.formatter.ElasticsearchZKFormatter;
import org.apache.mesos.elasticsearch.common.zookeeper.parser.ZKAddressParser;

import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Parses ZooKeeper information
 */
public class ParseZooKeeper implements TaskParser<String> {

    private static final Logger LOGGER = Logger.getLogger(ParseZooKeeper.class.getCanonicalName());

    @Override
    public String parse(Protos.TaskInfo taskInfo) throws InvalidParameterException {
        int nargs = taskInfo.getExecutor().getCommand().getArgumentsCount();
        LOGGER.info("Using arguments [" + nargs + "]: " + taskInfo.getExecutor().getCommand().getArgumentsList().toString());
        Map<String, String> argMap = new HashMap<>(1);
        Iterator<String> itr = taskInfo.getExecutor().getCommand().getArgumentsList().iterator();
        String lastKey = "";
        while (itr.hasNext()) {
            String value = itr.next();
            // If it is a argument command
            if (value.charAt(0) == '-') {
                lastKey = value;
            } else { // Else it must be an argument parameter
                argMap.put(lastKey, value);
            }
        }
        String address = argMap.get(ZooKeeper.ZOOKEEPER_ARG);
        if (address == null) {
            throw new InvalidParameterException("The task must pass a ZooKeeper address argument using " + ZooKeeper.ZOOKEEPER_ARG + ".");
        }

        ElasticsearchZKFormatter zkFormatter = new ElasticsearchZKFormatter(new ZKAddressParser());

        return zkFormatter.format(address);
    }
}
