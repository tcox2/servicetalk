
package io.servicetalk.concurrent.api;

import io.servicetalk.concurrent.Cancellable;
import io.servicetalk.concurrent.PublisherSource.Subscriber;
import io.servicetalk.concurrent.PublisherSource.Subscription;
import io.servicetalk.concurrent.api.ThreadSafeBuffer.Flush;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class BufferedPublisher<T> extends Publisher<Iterable<T>> {

    private final Consumer<Subscriber<? super T>> source;
    private final int targetChunkSize;
    private final Function<Runnable, Timeout> timeouts;
    private final BufferInstrumentation<T> instrumentation;


    public BufferedPublisher(Consumer<Subscriber<? super T>> source,
                             int targetChunkSize,
                             Function<Runnable, Timeout> timeouts,
                             BufferInstrumentation<T> instrumentation) {
        requireNonNull(source);
        requireNonNull(timeouts);
        requireNonNull(instrumentation);

        if (targetChunkSize < 1) {
            throw new IllegalArgumentException("targetChunkSize must be one or greater");
        }

        this.source = source;
        this.targetChunkSize = targetChunkSize;
        this.timeouts = timeouts;
        this.instrumentation = instrumentation;

    }

    @Override
    protected void handleSubscribe(Subscriber<? super Iterable<T>> subscriber) {
        instrumentation.subscribed();
        BufferedSubscription subscription = new BufferedSubscription();
        subscription.subscribe(subscriber);
    }

    public class BufferedSubscription {

        private final ThreadSafeBuffer<T> buffer = new ThreadSafeBuffer<>(targetChunkSize);
        private Subscription itemsSubscription = new NullSubscription();
        private Subscriber<? super Iterable<T>> chunksSubscriber = new NullSubscriber<>();
        private final Filler filler = new Filler();

        public void subscribe(Subscriber<? super Iterable<T>> destination) {
            source.accept(filler);
            chunksSubscriber = destination;
            chunksSubscriber.onSubscribe(new Drainer());
        }

        class Filler implements Subscriber<T> {

            private final Timeout timeout = timeouts.apply(this::timeout);

            Filler() {
                timeout.start();
            }

            @Override
            public void onSubscribe(Subscription subscription) {
                itemsSubscription = subscription;
            }

            @Override
            public void onNext(@Nullable T item) {
                buffer(item);
            }

            @Override
            public void onError(Throwable error) {
                try {
                    chunksSubscriber.onError(error);
                } finally {
                    timeout.stop();
                }
            }

            @Override
            public void onComplete() {
                try {
                    flush("onComplete");
                    chunksSubscriber.onComplete();
                } finally {
                    timeout.stop();
                }
            }

            private void timeout() {
                flush("timeout");
            }

            private void flush(String cause) {
                try {
                    List<T> flushed = buffer.enqueue(null, Flush.ALWAYS);
                    if (!flushed.isEmpty()) {
                        chunksSubscriber.onNext(flushed);
                    }
                    instrumentation.flushed(buffer, flushed.size(), cause);
                } finally {
                    timeout.stop();
                    timeout.start();
                }
            }

            private void buffer(@Nullable T item) {
                List<T> flushed = buffer.enqueue(item, Flush.WHEN_FULL);
                if (!flushed.isEmpty()) {
                    chunksSubscriber.onNext(flushed);
                }
                instrumentation.enqueued(buffer, item, flushed.size());
            }
        }

        class Drainer implements Subscription {

            @Override
            public void cancel() {
                // no more chunks wanted
                instrumentation.cancelled();
                itemsSubscription.cancel();
            }

            @Override
            public void request(long chunksCount) {
                long itemsCount = chunksToItems(chunksCount);
                instrumentation.requested(itemsCount, chunksCount);
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

    interface Timeout {
        void start();
        void stop();
    }

    static class NoTimeout implements Timeout {
        @Override
        public void start() {
        }

        @Override
        public void stop() {
        }
    }

    static class ExecutorTimeout implements Timeout {
        private final Executor executor;
        private final Duration duration;
        private final Runnable runnable;

        private volatile Cancellable cancellable = () -> {};

        ExecutorTimeout(Executor executor, Duration duration, Runnable runnable) {
            requireNonNull(executor);
            requireNonNull(duration);
            requireNonNull(runnable);
            this.executor = executor;
            this.duration = duration;
            this.runnable = runnable;
        }

        @Override
        public void start() {
            cancellable = executor.schedule(runnable, duration);
        }

        @Override
        public void stop() {
            cancellable.cancel();
        }
    }

    interface BufferInstrumentation<T> {
        default void subscribed() {
        }

        default void enqueued(ThreadSafeBuffer<T> buffer, T item, int flushedItemCount) {
        }

        default void flushed(ThreadSafeBuffer<T> buffer, int flushedItemCount, String cause) {
        }

        default void cancelled() {
        }

        default void requested(long itemsCount, long chunksCount) {
        }

    }

    public static class NullBufferInstrumentation<T> implements BufferInstrumentation<T> {
    }

    public static class LoggingBufferInstrumentation<T> implements BufferInstrumentation<T> {
        private final Logger logger = LoggerFactory.getLogger("Buffer");

        @Override
        public void subscribed() {
            logger.info("Subscribed");
        }

        @Override
        public void enqueued(ThreadSafeBuffer<T> buffer, T item, int flushedItemCount) {
            logger.info("[{}] Enqueued item [{}]{}", buffer, item,
                    flushedItemCount < 1 ? "" : " causing a flush of [" + flushedItemCount + "] items");
        }

        @Override
        public void flushed(ThreadSafeBuffer<T> buffer, int flushedItemCount, String cause) {
            logger.info("[{}] Explicit flush of {} items due to [{}]", buffer, flushedItemCount, cause);
        }

        @Override
        public void cancelled() {
            logger.info("Cancelled");
        }

        @Override
        public void requested(long itemsCount, long chunksCount) {
            logger.info("Request for [{}] chunks became request for [{}] items", format(chunksCount), format(itemsCount));
        }

        private static String format(long value) {
            if (value == Long.MAX_VALUE) {
                return "Long.MAX_VALUE";
            }
            return "" + value;
        }
    }

}
