package io.servicetalk.concurrent.api;

import io.servicetalk.concurrent.PublisherSource;
import io.servicetalk.concurrent.PublisherSource.Subscriber;
import io.servicetalk.concurrent.api.BufferedPublisher.Logging;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import sun.management.counter.LongCounter;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.servicetalk.concurrent.api.ImmediateExecutor.IMMEDIATE_EXECUTOR;
import static org.junit.jupiter.api.Assertions.*;

public class PublisherBufferTest2 {

    @Test
    public void splitToPairs() throws Exception {
        Publisher<Integer> input = Publisher.from(1, 2, 3, 4, 5);
        Publisher<Iterable<Integer>> actual = input.buffer(2, new Logging<Integer>());
        assertEquals("[[1, 2], [3, 4], [5]]", actual.toFuture().get().toString());
    }

    @Test
    public void splitToSizeOfSource() throws Exception {
        Publisher<Integer> input = Publisher.from(1, 2, 3, 4, 5);
        Publisher<Iterable<Integer>> actual = input.buffer(5, new Logging<>());
        assertEquals("[[1, 2, 3, 4, 5]]", actual.toFuture().get().toString());
    }

    @Test
    public void splitToOne() throws Exception {
        Publisher<Integer> input = Publisher.from(1, 2, 3, 4, 5);
        Publisher<Iterable<Integer>> actual = input.buffer(1, new Logging<>());
        assertEquals("[[1], [2], [3], [4], [5]]", actual.toFuture().get().toString());
    }

    @Test
    public void splitToZero() {
        Publisher<Integer> input = Publisher.from(1, 2, 3, 4, 5);

        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            input.buffer(0, new Logging<>());
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
                publisher.buffer(2, Duration.ofSeconds(1), IMMEDIATE_EXECUTOR, new Logging<>()).subscribeInternal(new Subscriber<Iterable<Integer>>() {

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

    class FakeException extends RuntimeException {
    }

    @Test
    // Make sure an error on the item publisher is passed through to
    public void errorPropagation() throws InterruptedException {
        AtomicBoolean errorSeen = new AtomicBoolean(false);
        final Object lock = new Object();
        final Publisher<String> bang = Publisher.failed(new FakeException());

        new Thread(new Runnable() {
            @Override
            public void run() {
                bang.subscribeInternal(new Subscriber<String>() {
                    @Override
                    public void onSubscribe(PublisherSource.Subscription subscription) {
                        subscription.request(1_000);
                    }

                    @Override
                    public void onNext(@Nullable String s) {
                        fail("onNext not expected");
                    }

                    @Override
                    public void onError(Throwable t) {
                        // expected
                        assertTrue(t instanceof FakeException);
                        errorSeen.set(true);
                        synchronized (lock) {
                            lock.notifyAll();
                        }
                    }

                    @Override
                    public void onComplete() {
                    }
                });


            }
        }).start();

        while (!errorSeen.get()) {
            synchronized (lock) {
                lock.wait(500);
            }
        }
    }

    // on-complete test?

    @Test
    public void multipleTimeout() {
        // How does this work?
        // The requested chunk size is big, so big that we would expect the input to yield one chunk
        // But it is forced by timeouts to break the input into lists of one item
        StringBuilder out = new StringBuilder();

        slowPublisher().buffer(1_000,
                // comment out line below to see this test fail because of no timeout
                Duration.ofMillis(100), IMMEDIATE_EXECUTOR,
                new Logging<>()).subscribeInternal(
                new Subscriber<Iterable<Integer>>() {
                     @Override
                     public void onSubscribe(PublisherSource.Subscription subscription) {
                         subscription.request(1_000);
                     }

                     @Override
                     public void onNext(@Nullable Iterable<Integer> integers) {
                         out.append(integers);
                     }

                     @Override
                     public void onError(Throwable t) {
                         fail("onError not expected");
                     }

                     @Override
                     public void onComplete() {
                         System.out.println("onComplete");
                     }
                 }
        );

        // Note the separate chunks - this won't happen without the timeout
        assertEquals("[1][2][3][4][5]", out.toString());
    }

    private static Publisher<Integer> slowPublisher() {
        Object creationLock = new Object();
        AtomicReference<Publisher<Integer>> slow = new AtomicReference<>();
        new Thread(new Runnable() {
            @Override
            public void run() {
                AtomicInteger integer = new AtomicInteger(0);
                slow.set( Publisher.fromIterable(new Iterable<Integer>() {
                    @Override
                    public Iterator<Integer> iterator() {
                        return new Iterator<Integer>() {
                            @Override
                            public boolean hasNext() {
                                return integer.get() < 5;
                            }

                            @Override
                            public Integer next() {
                                while (true) {
                                    try {
                                        Thread.sleep(500);
                                        return integer.incrementAndGet();
                                    } catch (InterruptedException e) {
                                        throw new RuntimeException(e);
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
        while (slow.get() == null) {
            synchronized (creationLock) {
                try {
                    creationLock.wait(100);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return slow.get();
    }

}
