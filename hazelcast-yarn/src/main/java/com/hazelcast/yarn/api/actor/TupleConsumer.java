package com.hazelcast.yarn.api.actor;

import com.hazelcast.yarn.api.tuple.Tuple;
import com.hazelcast.yarn.api.ShufflingStrategy;
import com.hazelcast.yarn.api.tuple.io.TupleInputStream;

/***
 * This is an abstract interface for each consumer in the system
 * which consumes tuple
 */
public interface TupleConsumer extends Consumer<TupleInputStream> {
    /***
     * @param chunk - chunk of Tuples to consume
     * @return really consumed amount of tuples
     * @throws Exception
     */
    int consumeChunk(TupleInputStream chunk) throws Exception;

    /***
     * @param tuple - tuple to consume
     * @return 1 if tuple was consumed , 0 otherwise
     * @throws Exception
     */
    int consumeTuple(Tuple tuple) throws Exception;

    /***
     * @return true if consumer supports shuffling, false otherwise
     */
    boolean isShuffled();

    /***
     * Perfoms flush of last consumed data
     *
     * @return amount of flushed data
     */
    int flush();

    /***
     * @return true if last data has been flushed, false otherwise
     */
    boolean isFlushed();

    /***
     * Opens current consumer
     */
    void open();

    /***
     * Closes current consumer
     */
    void close();

    /***
     * @return last consumed tuple count
     */
    int lastConsumedCount();

    /***
     * @return tuple consumer's shuffling strategy
     * null if consumer doesn't support shuffling
     */
    ShufflingStrategy getShufflingStrategy();
}
