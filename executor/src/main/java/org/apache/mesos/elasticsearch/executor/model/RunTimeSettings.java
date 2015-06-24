package org.apache.mesos.elasticsearch.executor.model;

import org.elasticsearch.common.settings.ImmutableSettings;

import java.util.List;

/**
 * Interface to allow the launcher to add run time settings from parsers.
 */
public interface RunTimeSettings {
    ImmutableSettings.Builder getRuntimeSettings();
}
