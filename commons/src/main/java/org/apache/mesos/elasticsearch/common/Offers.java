package org.apache.mesos.elasticsearch.common;

import org.apache.mesos.Protos;

/**
 *
 */
public class Offers {

    public static Protos.Offer.Builder newOfferBuilder(String offerId, String hostname, String slave, Protos.FrameworkID frameworkID) {
        Protos.OfferID offerID = Protos.OfferID.newBuilder().setValue(offerId).build();
        Protos.SlaveID slaveID = Protos.SlaveID.newBuilder().setValue(slave).build();
        return Protos.Offer.newBuilder().setId(offerID).setFrameworkId(frameworkID).setSlaveId(slaveID).setHostname(hostname);
    }

}
