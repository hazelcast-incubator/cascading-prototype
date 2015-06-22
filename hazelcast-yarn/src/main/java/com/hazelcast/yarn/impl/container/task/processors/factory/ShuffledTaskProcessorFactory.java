package com.hazelcast.yarn.impl.container.task.processors.factory;

import com.hazelcast.yarn.api.dag.Vertex;
import com.hazelcast.yarn.api.actor.TupleConsumer;
import com.hazelcast.yarn.api.actor.TupleProducer;
import com.hazelcast.yarn.api.container.ContainerContext;
import com.hazelcast.yarn.api.container.task.TaskProcessor;
import com.hazelcast.yarn.api.processor.TupleContainerProcessor;
import com.hazelcast.yarn.impl.container.task.processors.shuffling.ShuffledActorTaskProcessor;
import com.hazelcast.yarn.impl.container.task.processors.shuffling.ShuffledConsumerTaskProcessor;
import com.hazelcast.yarn.impl.container.task.processors.shuffling.ShuffledReceiverConsumerTaskProcessor;

public class ShuffledTaskProcessorFactory extends DefaultTaskProcessorFactory {
    @Override
    public TaskProcessor consumerTaskProcessor(TupleConsumer[] consumers, TupleContainerProcessor processor, ContainerContext containerContext, Vertex vertex, int taskID) {
        return new ShuffledConsumerTaskProcessor(consumers, processor, containerContext, taskID);
    }

    @Override
    public TaskProcessor actorTaskProcessor(TupleProducer[] producers, TupleConsumer[] consumers, TupleContainerProcessor processor, ContainerContext containerContext, Vertex vertex, int taskID) {
        return new ShuffledActorTaskProcessor(
                producers,
                processor,
                containerContext,
                consumerTaskProcessor(consumers, processor, containerContext, vertex, taskID),
                new ShuffledReceiverConsumerTaskProcessor(consumers, processor, containerContext, taskID),
                taskID
        );
    }

    public TaskProcessor getTaskProcessor(TupleProducer[] producers, TupleConsumer[] consumers, ContainerContext containerContext, TupleContainerProcessor processor, Vertex vertex, int taskID) {
        return actorTaskProcessor(producers, consumers, processor, containerContext, vertex, taskID);
    }
}
