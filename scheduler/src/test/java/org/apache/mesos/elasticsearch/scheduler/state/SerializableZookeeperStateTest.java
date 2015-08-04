package org.apache.mesos.elasticsearch.scheduler.state;

import org.apache.mesos.state.State;
import org.apache.mesos.state.Variable;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectOutputStream;
import java.nio.charset.Charset;
import java.security.InvalidParameterException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.*;

/**
 * Tests
 */
@SuppressWarnings({"PMD.TooManyMethods"})
public class SerializableZookeeperStateTest {
    public static final String SERIALIZABLE_OBJECT = "Serializable object";
    private State state;
    private Future<Variable> future;
    private SerializableState serializableState;
    private Variable variable;

    @Before
    public void before() throws IOException {
        state = mock(State.class);
        variable = mock(Variable.class);
        when(variable.value()).thenReturn(writeVariable(SERIALIZABLE_OBJECT));
        future = CompletableFuture.completedFuture(variable);
        serializableState = new SerializableZookeeperState(state);
        when(state.fetch(anyString())).thenReturn(future);
        when(state.store(any(Variable.class))).thenReturn(future);
    }

    @Test
    public void testSetValid() throws NotSerializableException {
        serializableState.set("test", "Serializable object");
        verify(state, times(1)).store(any(Variable.class));
    }

    @Test(expected = NotSerializableException.class)
    public void interrupted() throws NotSerializableException {
        when(state.store(any(Variable.class))).thenThrow(InterruptedException.class);
        serializableState.set("test", "Serializable object");
    }

    @Test(expected = NotSerializableException.class)
    public void executionException() throws NotSerializableException {
        when(state.store(any(Variable.class))).thenThrow(ExecutionException.class);
        serializableState.set("test", "Serializable object");
    }

    @Test(expected = NotSerializableException.class)
    public void ioException() throws NotSerializableException {
        when(state.store(any(Variable.class))).thenThrow(IOException.class);
        serializableState.set("test", "Serializable object");
    }

    @Test
    public void testGetValid() throws NotSerializableException {
        String variable = serializableState.get("test");
        verify(state, times(1)).fetch(anyString());
        assertEquals(SERIALIZABLE_OBJECT, variable);
    }

    @Test
    public void testNullIfNodeDoesntExist() throws IOException {
        when(variable.value()).thenReturn(new byte[0]);
        Object variable = serializableState.get("test");
        verify(state, times(1)).fetch(anyString());
        assertNull(variable);
    }

    @Test(expected = NotSerializableException.class)
    public void testInterrupted() throws NotSerializableException {
        when(state.fetch(anyString())).thenThrow(InterruptedException.class);
        serializableState.get("test");
    }

    @Test(expected = NotSerializableException.class)
    public void testClassNotFound() throws NotSerializableException {
        when(state.fetch(anyString())).thenThrow(ClassNotFoundException.class);
        serializableState.get("test");
    }

    @Test(expected = NotSerializableException.class)
    public void testExecution() throws NotSerializableException {
        when(state.fetch(anyString())).thenThrow(ExecutionException.class);
        serializableState.get("test");
    }

    @Test(expected = NotSerializableException.class)
    public void testIOException() throws NotSerializableException {
        when(state.fetch(anyString())).thenThrow(IOException.class);
        serializableState.get("test");
    }

    @Test(expected = NotSerializableException.class)
    public void testInvalidObjectStream() throws NotSerializableException {
        when(variable.value()).thenReturn("Invalid stream of bytes".getBytes(Charset.forName("UTF-8")));
        serializableState.get("test");
    }

    @Test
    public void shouldDeleteKey() {
        serializableState.delete("test");
    }

    @Test(expected = InvalidParameterException.class)
    public void shouldExceptionIfKeyDoesntExist() throws IOException {
        when(variable.value()).thenReturn("".getBytes(Charset.forName("UTF-8")));
        future = CompletableFuture.completedFuture(variable);
        when(state.fetch(anyString())).thenReturn(future);
        serializableState.delete("test");
    }


    // Data must be serialized/deserialized in exactly the same way to be readable.
    private byte[] writeVariable(Object object) throws IOException {
        ByteArrayOutputStream bos = null;
        ObjectOutputStream out = null;
        try {
            bos = new ByteArrayOutputStream();
            out = new ObjectOutputStream(bos);
            out.writeObject(object);
            return bos.toByteArray();
        } finally {
            if (out != null) {
                out.close();
            }
            if (bos != null) {
                bos.close();
            }
        }
    }
}