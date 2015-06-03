package org.apache.mesos.elasticsearch.common;

import org.apache.mesos.Protos;

/**
 * Helper class for building Mesos resources.
 */
public class Resources {

    public static Protos.Resource portRange(long beginPort, long endPort) {
        Protos.Value.Range singlePortRange = Protos.Value.Range.newBuilder().setBegin(beginPort).setEnd(endPort).build();
        return Protos.Resource.newBuilder()
                .setName("ports")
                .setType(Protos.Value.Type.RANGES)
                .setRanges(Protos.Value.Ranges.newBuilder().addRange(singlePortRange))
                .build();
    }

    public static Protos.Resource singlePortRange(long port) {
        return portRange(port, port);
    }

    public static Protos.Resource cpus(double cpus) {
        return Protos.Resource.newBuilder()
                .setName("cpus")
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(cpus).build())
                .build();
    }

    public static Protos.Resource mem(double mem) {
        return Protos.Resource.newBuilder()
                .setName("mem")
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(mem).build())
                .build();
    }

    public static Protos.Resource disk(double disk) {
        return Protos.Resource.newBuilder()
                .setName("disk")
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(disk).build())
                .build();

    }
}
