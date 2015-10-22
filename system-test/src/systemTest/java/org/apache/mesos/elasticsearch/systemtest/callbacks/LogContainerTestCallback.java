package org.apache.mesos.elasticsearch.systemtest.callbacks;

import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.core.command.LogContainerResultCallback;
import org.apache.commons.io.IOUtils;

import java.io.*;

/**
 * Callback for logging of docker commands that require textual output
 */
public class LogContainerTestCallback extends LogContainerResultCallback {
    protected final StringBuffer log = new StringBuffer();

    @Override
    public void onNext(Frame frame) {
        try {
            log.append(IOUtils.toString(new ByteArrayInputStream(frame.getPayload()), "UTF-8"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String toString() {
        return log.toString();
    }
}
