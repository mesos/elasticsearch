package org.apache.mesos.elasticsearch.common.zookeeper.formatter;

/**
 * Interface for formatters
 */
public interface ZKFormatter {
    String format(String zkURL);
}
