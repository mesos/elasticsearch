package org.apache.mesos.elasticsearch.systemtest;

import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.core.command.LogContainerResultCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Callback for the log container command used in tests.
 */
public class LogCallback extends LogResultCallback {

    private final static Logger LOGGER = LoggerFactory.getLogger(LogContainerResultCallback.class);

    protected final StringBuffer log = new StringBuffer();

    @Override
    public void onNext(Frame item) {
        LOGGER.debug(item.toString());
        log.append(new String(item.getPayload()));
    }

    @Override
    public String toString() {
        return log.toString();
    }

}
