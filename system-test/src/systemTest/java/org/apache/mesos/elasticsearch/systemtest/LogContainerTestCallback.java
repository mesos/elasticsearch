package org.apache.mesos.elasticsearch.systemtest;

import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.core.async.ResultCallbackTemplate;
import com.github.dockerjava.core.command.LogContainerResultCallback;

/**
 * Callback for logging of docker commands that require textual output
 */
public class LogContainerTestCallback extends ResultCallbackTemplate<LogContainerResultCallback, Frame> {
    protected final StringBuffer log = new StringBuffer();

    @Override
    public void onNext(Frame frame) {
        log.append(new String(frame.getPayload()));
    }

    @Override
    public String toString() {
        return log.toString();
    }
}
