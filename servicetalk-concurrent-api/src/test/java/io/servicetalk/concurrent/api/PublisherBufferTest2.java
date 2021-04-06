package io.servicetalk.concurrent.api;

import io.servicetalk.concurrent.PublisherSource;
import io.servicetalk.concurrent.api.BufferedPublisher.LoggingBufferInstrumentation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

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

    @Test
    // todo: this works but could do with a cleanup
    public void timeout() {
        Object lock = new Object();
        Object creationLock = new Object();
        AtomicInteger current = new AtomicInteger(0);
        AtomicInteger max = new AtomicInteger(3);
        AtomicReference<Publisher<Integer>> heldBack = new AtomicReference<>();

        new Thread(new Runnable() {
            @Override
            public void run() {
                heldBack.set( Publisher.fromIterable(new Iterable<Integer>() {
                    @Override
                    public Iterator<Integer> iterator() {
                         return new Iterator<Integer>() {
                             @Override
                             public boolean hasNext() {
                                 return true;
                             }

                             @Override
                             public Integer next() {
                                 while (true) {
                                     if (current.get() < max.get()) {
                                         return current.incrementAndGet();
                                     }
                                     synchronized (lock) {
                                         try {
                                             lock.wait();
                                         } catch (InterruptedException e) {
                                             throw new RuntimeException(e);
                                         }
                                     }
                                 }
                             }
                         };
                    }
                }));
                synchronized (creationLock) {
                    creationLock.notifyAll();
                }
            }
        }).start();

        while (heldBack.get() == null) {
            synchronized (creationLock) {
                try {
                    creationLock.wait(500);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        Object consumeLock = new Object();
        List<Iterable<Integer>> consumed = new ArrayList<>();
        Publisher<Integer> publisher = heldBack.get();

        // consumer thread
        new Thread(new Runnable() {
            @Override
            public void run() {
                publisher.buffer(2, Duration.ofSeconds(1), IMMEDIATE_EXECUTOR, new LoggingBufferInstrumentation<>()).subscribeInternal(new PublisherSource.Subscriber<Iterable<Integer>>() {

                    @Override
                    public void onSubscribe(PublisherSource.Subscription subscription) {
                        System.out.println("onSubscribe");
                        subscription.request(1_000);
                    }

                    @Override
                    public void onNext(@Nullable Iterable<Integer> chunk) {
                        consumed.add(chunk);
                        synchronized (consumeLock) {
                            consumeLock.notifyAll();
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        fail(t);
                    }

                    @Override
                    public void onComplete() {
                        System.out.println("onComplete");
                    }
                });
            }
        }).start();

        while ( ! consumed.toString().contains("3")) { // will happen only because of a timeout flush
            synchronized (consumeLock) {
                try {
                    consumeLock.wait(500);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            System.out.println("Consumed so far: " + consumed);
        }
    }

}
