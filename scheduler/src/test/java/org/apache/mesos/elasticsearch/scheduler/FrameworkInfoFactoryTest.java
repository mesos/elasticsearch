package org.apache.mesos.elasticsearch.scheduler;

import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.scheduler.state.FrameworkState;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests
 */
public class FrameworkInfoFactoryTest {

    public static final int DUMMY_PORT = 1234;

    public static final String DUMMY_FRAMEWORK_ROLE = "SomeFrameworkRole";
    private FrameworkState frameworkState = mock(FrameworkState.class);

    @Test
    public void shouldGetBuilder() {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getFrameworkName()).thenReturn("TestFrameworkName");
        when(configuration.getWebUiPort()).thenReturn(DUMMY_PORT);
        when(configuration.getFrameworkRole()).thenReturn(DUMMY_FRAMEWORK_ROLE);
        FrameworkInfoFactory frameworkInfoFactory = new FrameworkInfoFactory(configuration, frameworkState);
        Protos.FrameworkInfo frameworkInfo = frameworkInfoFactory.getBuilder().build();
        assertTrue(frameworkInfo.getWebuiUrl().contains("http://"));
        assertTrue(frameworkInfo.getWebuiUrl().contains(Integer.toString(DUMMY_PORT)));
        assertEquals(DUMMY_FRAMEWORK_ROLE, frameworkInfo.getRole());
    }
}