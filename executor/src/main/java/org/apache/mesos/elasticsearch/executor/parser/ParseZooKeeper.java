package org.apache.mesos.elasticsearch.executor.parser;

import org.apache.log4j.Logger;
import org.apache.mesos.Protos;

import java.security.InvalidAlgorithmParameterException;
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
    public String parse(Protos.TaskInfo taskInfo) throws InvalidAlgorithmParameterException {
        int nargs = taskInfo.getExecutor().getCommand().getArgumentsCount();
        LOGGER.info("Using arguments [" + nargs + "]: " + taskInfo.getExecutor().getCommand().getArgumentsList().toString());
        if (nargs > 0 && nargs % 2 == 0) {
            Map<String, String> argMap = new HashMap<>(1);
            Iterator<String> itr = taskInfo.getExecutor().getCommand().getArgumentsList().iterator();
            while (itr.hasNext()) {
                argMap.put(itr.next(), itr.next());
            }
            return argMap.get("-zk");
        } else {
            throw new InvalidParameterException("The task must pass a ZooKeeper address argument using -zk.");
        }
    }
}
