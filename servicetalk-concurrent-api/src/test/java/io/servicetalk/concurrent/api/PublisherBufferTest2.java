package io.servicetalk.concurrent.api;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PublisherBufferTest2 {

    @Test
    public void basic() throws Exception {
        Publisher<Integer> input = Publisher.from(1, 2, 3, 4);
        Publisher<Iterable<Integer>> actual = input.buffer(2, Duration.ofSeconds(1));
        actual.toFuture().get().toString();
    }

}
