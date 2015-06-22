package com.hazelcast.yarn.api.tuple;

import com.hazelcast.yarn.api.actor.ProducerCompletionHandler;
import com.hazelcast.yarn.api.actor.TupleActor;

public interface CompletionAwareProducer {
    void registerCompletionHandler(ProducerCompletionHandler runnable);

    void handleProducerCompleted();
}
