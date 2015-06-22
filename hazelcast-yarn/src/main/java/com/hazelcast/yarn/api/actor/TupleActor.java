package com.hazelcast.yarn.api.actor;

import com.hazelcast.yarn.api.container.ContainerTask;

/***
 * This is an abstract interface for each actor in the system which
 * consume or produce tuple
 */
public interface TupleActor extends TupleProducer, TupleConsumer {
    /***
     * @return container's task it belongs to
     */
    ContainerTask getTask();
}
