package io.servicetalk.concurrent.api;

import org.junit.jupiter.api.Test;

import java.time.Duration;

public class PublisherBufferTest2 {

    @Test
    public void basic() throws Exception {
        Publisher<Integer> input = Publisher.from(1, 2, 3, 4, 5);
        Publisher<Iterable<Integer>> actual = input.buffer(2, Duration.ofMillis(500));
        actual.toFuture().get().toString();
    }

}
