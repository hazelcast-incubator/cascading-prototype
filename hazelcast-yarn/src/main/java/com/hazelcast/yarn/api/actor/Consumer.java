package com.hazelcast.yarn.api.actor;

/***
 * This is an abstract interface for each consumer
 */
public interface Consumer<T> {
    /***
     * Method to consume an abstract entry
     *
     * @param entry - entity to consume
     * @return true if entry has been consumed
     * false if entry hasn't been consumed
     */
    boolean consume(T entry) throws Exception;
}
