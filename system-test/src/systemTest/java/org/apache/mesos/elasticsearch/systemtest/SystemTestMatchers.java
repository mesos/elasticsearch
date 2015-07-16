package org.apache.mesos.elasticsearch.systemtest;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.time.DateTimeException;
import java.time.ZonedDateTime;

/**
 * Hamcrest matchers for use in tests
 */
public class SystemTestMatchers {
    public static Matcher<? super String> isValidAddress() {
        return new TypeSafeMatcher<String>() {
            @Override
            protected boolean matchesSafely(String item) {
                return item.matches("^[a-zA-Z0-9\\.\\-]+(:[0-9]+)?$");
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("host[:port]");
            }
        };
    }

    public static Matcher<? super String> isValidDateTime() {
        return new TypeSafeMatcher<String>() {
            @Override
            protected boolean matchesSafely(String item) {
                try {
                    ZonedDateTime.parse(item);
                    return true;
                } catch (DateTimeException e) {
                    return false;
                }
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("Valid ISO zoned date time");
            }
        };
    }
}
