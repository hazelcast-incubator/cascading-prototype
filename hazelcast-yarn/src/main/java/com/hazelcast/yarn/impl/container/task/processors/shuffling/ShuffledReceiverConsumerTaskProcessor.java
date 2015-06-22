package com.hazelcast.yarn.impl.container.task.processors.shuffling;

import com.hazelcast.yarn.api.actor.TupleConsumer;
import com.hazelcast.yarn.api.container.ContainerContext;
import com.hazelcast.yarn.api.processor.TupleContainerProcessor;

public class ShuffledReceiverConsumerTaskProcessor extends ShuffledConsumerTaskProcessor {
    public ShuffledReceiverConsumerTaskProcessor(TupleConsumer[] consumers,
                                                 TupleContainerProcessor processor,
                                                 ContainerContext containerContext,
                                                 int taskID) {
        super(consumers, processor, containerContext, taskID, true);
    }
}
