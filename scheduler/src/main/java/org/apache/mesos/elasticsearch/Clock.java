package org.apache.mesos.elasticsearch;

import java.util.Date;

/**
 * Used for mocking time related code.
 */
public class Clock {

    public Date now() {
        return new Date();
    }

}
