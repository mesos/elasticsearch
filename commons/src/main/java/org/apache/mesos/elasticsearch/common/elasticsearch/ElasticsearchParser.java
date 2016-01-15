package org.apache.mesos.elasticsearch.common.elasticsearch;

import org.json.JSONObject;

/**
 */
public class ElasticsearchParser {
    public static final String HTTP_ADDRESS = "http_address";

    public static String parseHttpAddress(JSONObject task) {
        return task.getString(HTTP_ADDRESS);
    }
}
