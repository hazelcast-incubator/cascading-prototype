package com.hazelcast.yarn.impl.container.task.processors.factory;

import com.hazelcast.yarn.api.dag.Vertex;
import com.hazelcast.yarn.api.actor.TupleConsumer;
import com.hazelcast.yarn.api.actor.TupleProducer;
import com.hazelcast.yarn.api.container.ContainerContext;
import com.hazelcast.yarn.api.container.task.TaskProcessor;
import com.hazelcast.yarn.api.processor.TupleContainerProcessor;
import com.hazelcast.yarn.api.container.task.TaskProcessorFactory;
import com.hazelcast.yarn.impl.container.task.processors.ActorTaskProcessor;
import com.hazelcast.yarn.impl.container.task.processors.SimpleTaskProcessor;
import com.hazelcast.yarn.impl.container.task.processors.ConsumerTaskProcessor;
import com.hazelcast.yarn.impl.container.task.processors.ProducerTaskProcessor;

import static com.hazelcast.util.Preconditions.checkNotNull;

public class DefaultTaskProcessorFactory implements TaskProcessorFactory {
    @Override
    public TaskProcessor simpleTaskProcessor(TupleContainerProcessor processor, ContainerContext containerContext, Vertex vertex, int taskID) {
        return new SimpleTaskProcessor(processor, containerContext, taskID);
    }

    @Override
    public TaskProcessor consumerTaskProcessor(TupleConsumer[] consumers, TupleContainerProcessor processor, ContainerContext containerContext, Vertex vertex, int taskID) {
        return new ConsumerTaskProcessor(consumers, processor, containerContext);
    }

    @Override
    public TaskProcessor producerTaskProcessor(TupleProducer[] producers, TupleContainerProcessor processor, ContainerContext containerContext, Vertex vertex, int taskID) {
        return new ProducerTaskProcessor(producers, processor, containerContext, taskID);
    }

    @Override
    public TaskProcessor actorTaskProcessor(TupleProducer[] producers, TupleConsumer[] consumers, TupleContainerProcessor processor, ContainerContext containerContext, Vertex vertex, int taskID) {
        return new ActorTaskProcessor(producers, processor, containerContext, consumerTaskProcessor(consumers, processor, containerContext, vertex, taskID), taskID);
    }

    public TaskProcessor getTaskProcessor(TupleProducer[] producers,
                                          TupleConsumer[] consumers,
                                          ContainerContext containerContext,
                                          TupleContainerProcessor processor,
                                          Vertex vertex,
                                          int taskID) {
        checkNotNull(producers);
        checkNotNull(consumers);
        checkNotNull(containerContext);
        checkNotNull(processor);
        checkNotNull(vertex);

        if (producers.length == 0) {
            if (consumers.length == 0)
                return simpleTaskProcessor(processor, containerContext, vertex, taskID);
            else
                return consumerTaskProcessor(consumers, processor, containerContext, vertex, taskID);
        } else {
            if (consumers.length == 0)
                return producerTaskProcessor(producers, processor, containerContext, vertex, taskID);
            else
                return actorTaskProcessor(producers, consumers, processor, containerContext, vertex, taskID);
        }
    }
}
