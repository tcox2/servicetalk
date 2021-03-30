
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

    public enum Cause {

        ON_NEXT(false),
        ON_COMPLETE(true),
        TIME_OUT(false);

        private final boolean terminal;

        Cause(boolean terminal) {
            this.terminal = terminal;
        }

        boolean isTerminal() {
            return terminal;
        }

    }

    /**
     * Enqueues an item into the buffer
     *
     * @param item to be enqueued, or null if none
     * @param mode flush when full or always
     * @return items flushed because size was met or exceeded (length may not be size)
     */
    public @Nullable List<T> enqueue(@Nullable T item, Flush mode, Cause cause) {
        synchronized (lock) {
            if (item != null) {
                buffer.add(item);
                log("Enqueued: {}", item);
            }

            if (shouldFlush(mode, buffer.size())) {
                List<T> clone = new ArrayList<>(buffer);
                buffer.clear();
                log("Flushed with mode {} because {}: {} items ", mode, cause, clone.size());
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

    private void log(String format, Object... args) {
        // TODO: Log level wants to be finer
        LOGGER.info(state() + " " + format, args);
    }

    private String state() {
        return String.format("[%d/%d]", buffer.size(), maximumSize);
    }

}
