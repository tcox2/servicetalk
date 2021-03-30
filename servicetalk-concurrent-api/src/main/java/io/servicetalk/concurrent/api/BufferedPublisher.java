
package io.servicetalk.concurrent.api;

import io.servicetalk.concurrent.PublisherSource.Subscriber;
import io.servicetalk.concurrent.PublisherSource.Subscription;
import io.servicetalk.concurrent.api.ThreadSafeBuffer.Cause;
import io.servicetalk.concurrent.api.ThreadSafeBuffer.Flush;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;


import static java.util.Objects.requireNonNull;

// TODO:
// timed flushing / maximum delay / thread to make them happen
// JDK 9? multiply exact?
// Log level on tsb
// should exceptions re-thrown, onError to onError, be nested?
// tests with exceptions onError etc.
// TIMING FLUSH ON TIME
// ScheduledThreadPoolExecutor?
// log level


public class BufferedPublisher<T> extends Publisher<Iterable<T>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BufferedPublisher.class);

    private final Consumer<Subscriber<? super T>> source;
    private final int targetChunkSize;
    private final ScheduledThreadPoolExecutor scheduler;
    private final Duration maximumDelay;

    public BufferedPublisher(Consumer<Subscriber<? super T>> source,
                             int targetChunkSize,
                             Duration maximumDelay,
                             ScheduledThreadPoolExecutor scheduler) {
        requireNonNull(maximumDelay);
        requireNonNull(source);
        requireNonNull(scheduler);
        requireNonNull(maximumDelay);

        if (targetChunkSize < 1) {
            throw new IllegalArgumentException("targetChunkSize must be one or greater");
        }

        this.source = source;
        this.targetChunkSize = targetChunkSize;

        // buffer needs a 'last flush time' on it
        // how to ensure re-schedule while also having reset() when a flush happens
        // you can get queue and clear it?
        // schedul at fixed blah, then clear+repopulate on another type of flush?
        // on vcomplete or error -> tear down
        // you get a schedlewd future!  xcan cancel it!
        // executor.schedule()
        this.scheduler = scheduler;
        this.maximumDelay = maximumDelay;
    }

    @Override
    protected void handleSubscribe(Subscriber<? super Iterable<T>> subscriber) {
        BufferedSubscription subscription = new BufferedSubscription();
        subscription.subscribe(subscriber);
    }

    public class BufferedSubscription {

        private final ThreadSafeBuffer<T> buffer = new ThreadSafeBuffer<>(targetChunkSize);
        private Subscription itemsSubscription = new NullSubscription();
        private Subscriber<? super Iterable<T>> chunksSubscriber = new NullSubscriber<>();
        private Runnable cancelTimeout = () -> {};
        private final Filler filler = new Filler();

        public void subscribe(Subscriber<? super Iterable<T>> destination) {
            scheduleTimeout();
            source.accept(filler);
            chunksSubscriber = destination;
            chunksSubscriber.onSubscribe(new Drainer());
        }



        private void resetTimeout(Cause cause) {
            cancelTimeout.run();

            if ( ! cause.isTerminal()) {
                scheduleTimeout();
            }
        }

        private void scheduleTimeout() {
            ScheduledFuture<?> scheduledFuture = scheduler.schedule(() -> filler.flush(Cause.TIME_OUT), maximumDelay.toNanos(), TimeUnit.NANOSECONDS);
            cancelTimeout = () -> {
                if (scheduledFuture.cancel(false)) {
                    LOGGER.info("Timeout was cancelled");
                }
            };
            LOGGER.info("Timeout was scheduled");
        }

        class Filler implements Subscriber<T> {

            @Override
            public void onSubscribe(Subscription subscription) {
                itemsSubscription = subscription;
            }

            @Override
            public void onNext(@Nullable T item) {
                buffer(item, Flush.WHEN_FULL, Cause.ON_NEXT);

                // Timeout experiment - recreate in a test
                /*try {
                    Thread.sleep(1_000);
                } catch (InterruptedException e) {
                }*/
                // Timeout experiment - recreate in a test

            }

            @Override
            public void onError(Throwable error) {
                cancelTimeout.run();
                chunksSubscriber.onError(error);
            }

            @Override
            public void onComplete() {
                flush(Cause.ON_COMPLETE);
                chunksSubscriber.onComplete();
            }

            public void flush(Cause cause) {
                buffer(null, Flush.ALWAYS, cause);
            }

            private void buffer(@Nullable T item, Flush mode, Cause cause) {
                List<T> flushed = buffer.enqueue(item, mode, cause);
                if (flushed != null) {
                    resetTimeout(cause);
                    chunksSubscriber.onNext(flushed);
                }
            }

        }

        class Drainer implements Subscription {

            @Override
            public void cancel() {
                // no more chunks wanted
                itemsSubscription.cancel();
            }

            @Override
            public void request(long chunksCount) {
                long itemsCount = chunksToItems(chunksCount);
                itemsSubscription.request(itemsCount);
            }

            private long chunksToItems(long chunkCount) {
                try {
                    return Math.multiplyExact(chunkCount, targetChunkSize);
                } catch (ArithmeticException e) {
                    // Overflow is likely - Long.MAX_LONG is often used as an input
                    // to the the request method, and we are multiplying it
                    return Long.MAX_VALUE;
                }
            }

        }

    }

}
