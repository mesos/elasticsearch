package org.elasticsearch.cloud.mesos;

import org.apache.commons.io.IOUtils;
import org.elasticsearch.Version;
import org.elasticsearch.common.lang3.tuple.Pair;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.mesos.MesosUnicastHostsProvider;
import org.json.JSONObject;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests parsing of slave host and port numbers Mesos state file.
 */
public class MesosStateServiceRestTest {

    @Test
    public void testGetNodeIpsAndPorts() throws IOException {
        Settings settings = ImmutableSettings.builder().put("cloud.mesos.master", "master").build();

        MesosStateServiceRest mesosStateServiceRest = new MesosStateServiceRest(settings);

        MesosStateRepository repository = mock(MesosStateRepository.class);
        JSONObject jsonObject = new JSONObject(IOUtils.toString(new FileInputStream("src/test/resources/state.json")));
        when(repository.retrieveState()).thenReturn(jsonObject);
        mesosStateServiceRest.setRepository(repository);

        List<Pair<String, Integer>> nodeIpsAndPorts = mesosStateServiceRest.getNodeIpsAndPorts(new MesosUnicastHostsProvider(settings, mesosStateServiceRest, Version.V_1_4_0));

        assertThat(nodeIpsAndPorts.get(0).getKey(), is("ip-10-0-0-134.eu-west-1.compute.internal"));
        assertThat(nodeIpsAndPorts.get(0).getValue(), is(5053));

        assertThat(nodeIpsAndPorts.get(1).getKey(), is("ip-10-0-0-135.eu-west-1.compute.internal"));
        assertThat(nodeIpsAndPorts.get(1).getValue(), is(5053));

        assertThat(nodeIpsAndPorts.get(2).getKey(), is("ip-10-0-0-136.eu-west-1.compute.internal"));
        assertThat(nodeIpsAndPorts.get(2).getValue(), is(5053));
    }

}
