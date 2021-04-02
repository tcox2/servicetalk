
package io.servicetalk.concurrent.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class ThreadSafeBuffer<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ThreadSafeBuffer.class);

    private final Object lock = new Object();
    private final List<T> buffer;
    private final int maximumSize;

    ThreadSafeBuffer(int maximumSize) {
        this.buffer = new ArrayList<>(maximumSize);
        this.maximumSize = maximumSize;
    }

    public enum Flush {
        WHEN_FULL,
        ALWAYS
    }

    /**
     * Enqueues an item into the buffer
     *
     * @param item to be enqueued, or null if none
     * @param mode flush when full or always
     * @return items flushed because size was met or exceeded (length may not be size)
     */
    public @Nullable List<T> enqueue(@Nullable T item, Flush mode) {
        synchronized (lock) {
            if (item != null) {
                buffer.add(item);
            }

            if (shouldFlush(mode, buffer.size())) {
                List<T> clone = new ArrayList<>(buffer);
                buffer.clear();
                return clone;
            }

            return null;
        }
    }

    private boolean shouldFlush(Flush mode, int size) {
        if (mode == Flush.ALWAYS) {
            return true;
        } else if (mode == Flush.WHEN_FULL) {
            return size >= maximumSize;
        } else {
            throw new IllegalStateException();
        }
    }

}
