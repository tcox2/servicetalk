package io.servicetalk.concurrent.api;

import io.servicetalk.concurrent.PublisherSource;
import io.servicetalk.concurrent.api.BufferedPublisher.LoggingBufferInstrumentation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static io.servicetalk.concurrent.api.ImmediateExecutor.IMMEDIATE_EXECUTOR;
import static org.junit.jupiter.api.Assertions.*;

public class PublisherBufferTest2 {

    @Test
    public void splitToPairs() throws Exception {
        Publisher<Integer> input = Publisher.from(1, 2, 3, 4, 5);
        Publisher<Iterable<Integer>> actual = input.buffer(2, new LoggingBufferInstrumentation<Integer>());
        assertEquals("[[1, 2], [3, 4], [5]]", actual.toFuture().get().toString());
    }

    @Test
    public void splitToSizeOfSource() throws Exception {
        Publisher<Integer> input = Publisher.from(1, 2, 3, 4, 5);
        Publisher<Iterable<Integer>> actual = input.buffer(5, new LoggingBufferInstrumentation<>());
        assertEquals("[[1, 2, 3, 4, 5]]", actual.toFuture().get().toString());
    }

    @Test
    public void splitToOne() throws Exception {
        Publisher<Integer> input = Publisher.from(1, 2, 3, 4, 5);
        Publisher<Iterable<Integer>> actual = input.buffer(1, new LoggingBufferInstrumentation<>());
        assertEquals("[[1], [2], [3], [4], [5]]", actual.toFuture().get().toString());
    }

    @Test
    public void splitToZero() {
        Publisher<Integer> input = Publisher.from(1, 2, 3, 4, 5);

        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            input.buffer(0, new LoggingBufferInstrumentation<>());
        });
    }

    // instrumentation

    @Test
    public void timeout() throws Exception {
        Publisher<Integer> input = Publisher.from(1, 2, 3, 4, 5);
        final String actual = input.buffer(2, Duration.ofSeconds(1), IMMEDIATE_EXECUTOR, new LoggingBufferInstrumentation<>()).toFuture().get().toString();
        assertEquals("[[1, 2], [3, 4], [5]]", actual);
    }

    // tck

}
