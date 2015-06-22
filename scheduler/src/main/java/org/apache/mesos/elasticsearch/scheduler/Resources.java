package org.apache.mesos.elasticsearch.scheduler;

import org.apache.mesos.Protos;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;

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

    public static List<Integer> selectTwoPortsFromRange(List<Protos.Resource> offeredResources) {
        List<Integer> ports = new ArrayList<>();
        offeredResources.stream().filter(resource -> resource.getType().equals(org.apache.mesos.Protos.Value.Type.RANGES))
                .forEach(resource -> resource.getRanges().getRangeList().stream().filter(range -> ports.size() < 2).forEach(range -> {
                    ports.add((int) range.getBegin());
                    if (ports.size() < 2 && range.getBegin() != range.getEnd()) {
                        ports.add((int) range.getBegin() + 1);
                    }
                }));
        return ports;
    }

    public static List<Protos.Resource> buildFrameworkResources() {
        Protos.Resource cpus = Resources.cpus(Configuration.CPUS);
        Protos.Resource mem = Resources.mem(Configuration.MEM);
        Protos.Resource disk = Resources.disk(Configuration.DISK);
        return asList(cpus, mem, disk);
    }
}
