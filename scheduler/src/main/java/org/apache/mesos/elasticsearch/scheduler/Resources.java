package org.apache.mesos.elasticsearch.scheduler;

import org.apache.mesos.Protos;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

/**
 * Helper class for building Mesos resources.
 */
public class Resources {

    public static final String RESOURCE_PORTS = "ports";
    public static final String RESOURCE_CPUS = "cpus";
    public static final String RESOURCE_MEM = "mem";
    public static final String RESOURCE_DISK = "disk";

    private Resources() {

    }

    public static Protos.Resource portRange(long beginPort, long endPort, String frameworkRole) {
        Protos.Value.Range singlePortRange = Protos.Value.Range.newBuilder().setBegin(beginPort).setEnd(endPort).build();
        return Protos.Resource.newBuilder()
                .setName(RESOURCE_PORTS)
                .setType(Protos.Value.Type.RANGES)
                .setRanges(Protos.Value.Ranges.newBuilder().addRange(singlePortRange))
                .setRole(frameworkRole)
                .build();
    }

    public static Protos.Resource singlePortRange(long port, String frameworkRole) {
        return portRange(port, port, frameworkRole);
    }

    public static Protos.Resource cpus(double cpus, String frameworkRole) {
        return Protos.Resource.newBuilder()
                .setName(RESOURCE_CPUS)
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(cpus).build())
                .setRole(frameworkRole)
                .build();
    }

    public static Protos.Resource mem(double mem, String frameworkRole) {
        return Protos.Resource.newBuilder()
                .setName(RESOURCE_MEM)
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(mem).build())
                .setRole(frameworkRole)
                .build();
    }

    public static Protos.Resource disk(double disk, String frameworkRole) {
        return Protos.Resource.newBuilder()
                .setName(RESOURCE_DISK)
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(disk).build())
                .setRole(frameworkRole)
                .build();
    }

    public static List<Integer> selectTwoUnprivilegedPortsFromRange(List<Protos.Resource> offeredResources) {
        return offeredResources.stream()
                .filter(resource -> resource.getType().equals(Protos.Value.Type.RANGES))
                .flatMap(resource -> resource.getRanges().getRangeList().stream())
                .flatMapToInt(range -> IntStream.rangeClosed((int) range.getBegin(), (int) range.getEnd()))
                .filter(port -> port > 1024)
                .limit(2)
                .boxed()
                .collect(Collectors.toList());
    }

    public static ArrayList<Protos.Resource> buildFrameworkResources(Configuration configuration) {
        Protos.Resource cpus = Resources.cpus(configuration.getCpus() - configuration.getExecutorCpus(), configuration.getFrameworkRole());
        Protos.Resource mem = Resources.mem(configuration.getMem() - configuration.getExecutorMem(), configuration.getFrameworkRole());
        Protos.Resource disk = Resources.disk(configuration.getDisk(), configuration.getFrameworkRole());
        return new ArrayList<>(Arrays.asList(cpus, mem, disk));
    }
}
