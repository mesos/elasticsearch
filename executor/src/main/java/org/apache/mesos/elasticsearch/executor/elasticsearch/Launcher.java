package org.apache.mesos.elasticsearch.executor.elasticsearch;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;

/**
 * Abstraction of the ES node launching process to decouple launching code.
 */
public interface Launcher {
    Node launch();
    void addRuntimeSettings(Settings.Builder settings);
}
