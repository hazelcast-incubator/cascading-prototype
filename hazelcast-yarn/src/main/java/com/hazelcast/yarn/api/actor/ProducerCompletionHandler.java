package com.hazelcast.yarn.api.actor;

/***
 * Completion handler for producers
 */
public interface ProducerCompletionHandler {
    /***
     * Will be invoked in case if producer
     *
     * @param producer - producer which has been completed
     */
    void onComplete(TupleProducer producer);
}
