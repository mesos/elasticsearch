package org.apache.mesos.elasticsearch.scheduler;

import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests
 */
public class FrameworkInfoFactoryTest {
@Test
public void shouldGetBuilder() {
    Configuration configuration = mock(Configuration.class);
    when(configuration.getFrameworkName()).thenReturn("TestFrameworkName");
    FrameworkInfoFactory frameworkInfoFactory = new FrameworkInfoFactory(configuration);
    frameworkInfoFactory.getBuilder();
}
}