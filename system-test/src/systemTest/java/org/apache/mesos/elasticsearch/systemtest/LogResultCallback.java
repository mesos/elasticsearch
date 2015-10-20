package org.apache.mesos.elasticsearch.systemtest;

import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.core.async.ResultCallbackTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class LogResultCallback extends ResultCallbackTemplate<LogResultCallback, Frame> {

    private final static Logger LOGGER = LoggerFactory.getLogger(LogResultCallback.class);

    @Override
    public void onNext(Frame item) {
        LOGGER.debug(item.toString());
    }
}
