package com.hazelcast.yarn.api.actor;

/***
 * This is an abstract interface for each sleeping strategy in the system
 */
public interface SleepingStrategy {
    /***
     * Awaits next amount of nanoseconds depending on if
     *
     * @param wasPayLoad value
     */
    void await(boolean wasPayLoad);
}
