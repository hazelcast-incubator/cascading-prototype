package com.hazelcast.yarn.api.actor;

/***
 * This is an abstract interface for each shuffler in the system
 */
public interface Shuffler {
    /***
     * Flush current buffer of Shuffler
     *
     * @return amount of bytes has been flushed
     */
    int flush();

    /***
     * Check if last buffer has been flushed
     * * @return true if last data has been flushed
     * false - if last data hasn't been flushed
     */
    boolean isFlushed();
}
