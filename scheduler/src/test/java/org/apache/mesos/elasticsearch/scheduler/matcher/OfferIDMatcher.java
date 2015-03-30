package org.apache.mesos.elasticsearch.scheduler.matcher;

import org.apache.mesos.Protos;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;

/**
 * Matcher for {@link org.apache.mesos.Protos.OfferID}s.
 */
public class OfferIDMatcher extends BaseMatcher<Protos.OfferID> {

    private String value;

    public OfferIDMatcher(String value) {
        this.value = value;
    }

    @Override
    public boolean matches(Object o) {
        Protos.OfferID offerId = (Protos.OfferID) o;
        return offerId.getValue().equals(value);
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("Offer ID: " + value);
    }
}
