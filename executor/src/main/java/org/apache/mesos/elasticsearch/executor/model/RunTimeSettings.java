package org.apache.mesos.elasticsearch.executor.model;

import org.elasticsearch.common.settings.Settings;

/**
 * Interface to allow the launcher to add run time settings from parsers.
 */
public interface RunTimeSettings {
    Settings.Builder getRuntimeSettings();
}
