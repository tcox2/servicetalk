
package io.servicetalk.concurrent.api;

import io.servicetalk.concurrent.PublisherSource.Subscriber;
import io.servicetalk.concurrent.PublisherSource.Subscription;
import io.servicetalk.concurrent.api.ThreadSafeBuffer.Flush;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.*;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

// TODO:
// Thread safety
// timed flushing / maximum delay / thread to make them happen
// Cancellable on subscription
// JDK 9? multiply exact?
// Log level on tsb
// should exceptions re-thrown, onError to onError, be nested?
// tests with exceptions onError etc.

public class BufferedPublisher<T> extends Publisher<Iterable<T>> {

    private final Consumer<Subscriber<? super T>> source;
    private final int targetChunkSize;

    public BufferedPublisher(Consumer<Subscriber<? super T>> source,
                             int targetChunkSize,
                             Duration maximumDelay) {
        requireNonNull(maximumDelay);
        requireNonNull(source);

        if (targetChunkSize < 1) {
            throw new IllegalArgumentException("targetChunkSize must be one or greater");
        }

        this.source = source;
        this.targetChunkSize = targetChunkSize;
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

        public void subscribe(Subscriber<? super Iterable<T>> destination) {
            source.accept(new Filler());
            chunksSubscriber = destination;
            chunksSubscriber.onSubscribe(new Drainer());
        }

        class Filler implements Subscriber<T> {

            @Override
            public void onSubscribe(Subscription subscription) {
                itemsSubscription = subscription;
            }

            @Override
            public void onNext(@Nullable T item) {
                buffer(item, Flush.WHEN_FULL);
            }

            @Override
            public void onError(Throwable error) {
                chunksSubscriber.onError(error);
            }

            @Override
            public void onComplete() {
                buffer(null, Flush.ALWAYS);
                chunksSubscriber.onComplete();
            }

            /**
            // So that we may keep the locking in one place, and thus maybe a little more ordered and under control,
            // this is the only place where you may take the lock or interact with the buffer
            */
            private void buffer(@Nullable T item, Flush mode) {
                List<T> flushed = buffer.enqueue(item, mode);
                if (flushed != null) {
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
