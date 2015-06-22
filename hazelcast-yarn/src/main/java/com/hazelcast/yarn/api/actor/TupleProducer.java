package com.hazelcast.yarn.api.actor;

import com.hazelcast.yarn.api.dag.Vertex;
import com.hazelcast.yarn.api.tuple.Tuple;
import com.hazelcast.yarn.api.tuple.CompletionAwareProducer;

/***
 * This is an abstract interface for each producer in the system
 * which produce tuple
 */
public interface TupleProducer extends Producer<Tuple[]>, CompletionAwareProducer {
    /***
     * @return last produced tuple count
     */
    int lastProducedCount();

    /***
     * @return true if producer supports shuffling, false otherwise
     */
    boolean isShuffled();

    /***
     * @return corresponding vertex
     */
    Vertex getVertex();

    /***
     * @return producer's name
     */
    String getName();

    /***
     * @return true if producer is closed , false otherwise
     */
    boolean isClosed();

    /***
     * Open current producer
     */
    void open();


    /***
     * Close current producer
     */
    void close();
}
