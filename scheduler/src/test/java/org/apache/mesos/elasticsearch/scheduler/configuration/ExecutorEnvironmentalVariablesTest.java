package org.apache.mesos.elasticsearch.scheduler.configuration;

import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.scheduler.Configuration;
import org.junit.Test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.*;

/**
 * Tests
 */
public class ExecutorEnvironmentalVariablesTest {
    @Test
    public void ensureOver1GBHeapIs256MB() throws Exception {
        int ram = 2048;
        Configuration configuration = new Configuration();
        configuration.setMem(ram);
        ExecutorEnvironmentalVariables env = new ExecutorEnvironmentalVariables(configuration);

        for ( Protos.Environment.Variable var : env.getList()) {
            if (var.getName().equals(ExecutorEnvironmentalVariables.JAVA_OPTS)) {
                String val = var.getValue();
                Pattern pattern = Pattern.compile(".*-Xmx(\\d*)m");
                Matcher matcher = pattern.matcher(val);
                assertTrue(matcher.matches());
                assertEquals(Integer.toString(ram - 256), matcher.group(1));
            }
        }
    }

    @Test
    public void ensureUnder1GBIsLessThan256MB() throws Exception {
        int ram = 512;
        Configuration configuration = new Configuration();
        configuration.setMem(ram);
        ExecutorEnvironmentalVariables env = new ExecutorEnvironmentalVariables(configuration);

        for ( Protos.Environment.Variable var : env.getList()) {
            if (var.getName().equals(ExecutorEnvironmentalVariables.JAVA_OPTS)) {
                String val = var.getValue();
                Pattern pattern = Pattern.compile(".*-Xmx(\\d*)m");
                Matcher matcher = pattern.matcher(val);
                assertTrue(matcher.matches());
                assertEquals(Integer.toString(ram - ram/4), matcher.group(1));
                assertTrue(ram - Integer.valueOf(matcher.group(1)) < 256);
                assertTrue(ram - Integer.valueOf(matcher.group(1)) < ram);
                assertTrue(ram - Integer.valueOf(matcher.group(1)) > 0);
            }
        }
    }
}