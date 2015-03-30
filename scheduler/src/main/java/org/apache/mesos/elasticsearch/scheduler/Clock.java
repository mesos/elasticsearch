package org.apache.mesos.elasticsearch.scheduler;

import java.util.Date;

/**
 * Used for mocking time related code.
 */
public class Clock {

    public Date now() {
        return new Date();
    }

}
