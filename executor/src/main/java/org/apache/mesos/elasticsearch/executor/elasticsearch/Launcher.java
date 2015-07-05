package org.apache.mesos.elasticsearch.executor.elasticsearch;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;

/**
 * Abstraction of the ES node launching process to decouple launching code.
 */
public interface Launcher {
    Node launch();
    void addRuntimeSettings(ImmutableSettings.Builder settings);
}
