package com.hazelcast.yarn.api.actor;


/***
 * This is an abstract interface for each producer
 */
public interface Producer<T> {
    /***
     * Method to produce an abstract entry
     *
     * @return produced entry
     */
    T produce();
}
