package org.apache.mesos.elasticsearch.scheduler;

import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.junit.Test;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.zip.ZipEntry;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Main test
 */
public class MainTest {
    @Test(expected = RuntimeException.class)
    public void testErrorIfNoHeap() throws Exception {
        Main.main(new String[0]);
    }

    @Test
    public void executorJarShouldBePresentInSchedulerJar() throws IOException {
        Path path = Paths.get("build/libs");

        // If this doesn't exist, then we must be using intellij junit runner
        if (!path.toFile().exists()) {
            path = Paths.get("scheduler/build/libs");
        }

        // If it still doesn't exist, then this means it hasn't been built yet. Skip test.
        if (!path.toFile().exists()) {
            return;
        }

        File dir = path.toFile();
        FileFilter fileFilter = new WildcardFileFilter("*.jar");
        File[] files = dir.listFiles(fileFilter);
        assertNotNull(path.toAbsolutePath() + " does not denote a directory", files);
        assertEquals("Jar file not found in " + path, 1, files.length);
        File schedulerJar = files[0];
        Boolean executorFound = false;

        try (JarFile jar = new JarFile(schedulerJar)) {
            // Getting the files into the jar
            Enumeration<? extends JarEntry> enumeration = jar.entries();
            // Iterates into the files in the jar file
            while (enumeration.hasMoreElements()) {
                ZipEntry zipEntry = enumeration.nextElement();

                // Is this a jar
                if (zipEntry.getName().endsWith(".jar")) {

                    // Relative path of file into the jar.
                    String jarName = zipEntry.getName();

                    // Is it the executor
                    if (jarName.contains("executor")) {
                        executorFound = true;
                        break;
                    }
                }
            }
        }

        assertTrue("Executor jar not found in " + schedulerJar.getPath(), executorFound);
    }
}