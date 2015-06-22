package com.hazelcast.yarn.impl;

import javax.annotation.Nullable;

public final class SettableFuture<V> extends AbstractFuture<V> {

    /**
     * Creates a new {@code SettableFuture} in the default state.
     */
    public static <V> SettableFuture<V> create() {
        return new SettableFuture<V>();
    }

    /**
     * Explicit private constructor, use the {@link #create} factory method to
     * create instances of {@code SettableFuture}.
     */
    private SettableFuture() {
    }

    /**
     * Sets the value of this future.  This method will return {@code true} if
     * the value was successfully set, or {@code false} if the future has already
     * been set or cancelled.
     *
     * @param value the value the future should hold.
     * @return true if the value was successfully set.
     */
    @Override
    public boolean set(@Nullable V value) {
        return super.set(value);
    }

    public void reset() {
        super.reset();
    }

    /**
     * Sets the future to having failed with the given exception. This exception
     * will be wrapped in an {@code ExecutionException} and thrown from the {@code
     * get} methods. This method will return {@code true} if the exception was
     * successfully set, or {@code false} if the future has already been set or
     * cancelled.
     *
     * @param throwable the exception the future should hold.
     * @return true if the exception was successfully set.
     */
    @Override
    public boolean setException(Throwable throwable) {
        return super.setException(throwable);
    }
}
