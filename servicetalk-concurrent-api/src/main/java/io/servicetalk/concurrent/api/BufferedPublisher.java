
package io.servicetalk.concurrent.api;

import io.servicetalk.concurrent.PublisherSource.Subscriber;
import io.servicetalk.concurrent.PublisherSource.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

// TODO:
// Thread safety
// timed flushing / maximum delay / thread to make them happen
// Cancellable on subscription
// JDK 9? multiply exact?
// Log level
// should exceptions re-thrown, onError to onError, be nested?

public class BufferedPublisher<T> extends Publisher<Iterable<T>> {

    private final Consumer<Subscriber<? super T>> source;
    private final int targetChunkSize;

    private static final Logger LOGGER = LoggerFactory.getLogger(BufferedPublisher.class);

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

        private final List<T> buffer = new ArrayList<>(targetChunkSize);
        private Subscription itemsSubscription = new NullSubscription();
        private Subscriber<? super Iterable<T>> chunksSubscriber = new NullSubscriber<>();

        public void subscribe(Subscriber<? super Iterable<T>> destination) {
            source.accept(new Filler());
            chunksSubscriber = destination;
            chunksSubscriber.onSubscribe(new Drainer());
        }

        private void log(String format, Object... args) {
            // TODO: Log level wants to be lower
            LOGGER.info(state() + " " + format, args);
        }

        private String state() {
            return "[" + buffer.size() + "/" + targetChunkSize + "]";
        }

        class Filler implements Subscriber<T> {

            @Override
            public void onSubscribe(Subscription subscription) {
                itemsSubscription = subscription;
            }

            @Override
            public void onNext(@Nullable T item) {
                buffer.add(item);
                log("Received item: " + item);
                maybeFlush();
            }

            @Override
            public void onError(Throwable error) {
                chunksSubscriber.onError(error);
                log("Error propagated: {}", error);
            }

            @Override
            public void onComplete() {
                log("Completed items");
                chunksSubscriber.onComplete();
            }

            // TODO: thread safety
            private void maybeFlush() {
                if (buffer.size() < targetChunkSize) {
                    return;
                }

                // lock?  because buffer may accumulate after copy?
                List<T> copy = new ArrayList<T>(buffer);
                buffer.clear();
                // end lock?
                chunksSubscriber.onNext(copy);
                log("Flushed {} items", copy.size());
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
                log("Request for {} chunks satisfied by request for {} items", chunksCount, itemsCount);
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
