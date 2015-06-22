package com.hazelcast.yarn.api.container.task;

import com.hazelcast.yarn.api.actor.TupleConsumer;
import com.hazelcast.yarn.api.actor.TupleProducer;
import com.hazelcast.yarn.api.container.ContainerContext;
import com.hazelcast.yarn.api.dag.Vertex;
import com.hazelcast.yarn.api.processor.TupleContainerProcessor;

public interface TaskProcessorFactory {
    TaskProcessor simpleTaskProcessor(TupleContainerProcessor processor, ContainerContext containerContext, Vertex vertex, int taskID);

    TaskProcessor consumerTaskProcessor(TupleConsumer[] consumers, TupleContainerProcessor processor, ContainerContext containerContext, Vertex vertex, int taskID);

    TaskProcessor producerTaskProcessor(TupleProducer[] producers, TupleContainerProcessor processor, ContainerContext containerContext, Vertex vertex, int taskID);

    TaskProcessor actorTaskProcessor(TupleProducer[] producers, TupleConsumer[] consumers, TupleContainerProcessor processor, ContainerContext containerContext, Vertex vertex, int taskID);

    TaskProcessor getTaskProcessor(TupleProducer[] producers, TupleConsumer[] consumers, ContainerContext containerContext, TupleContainerProcessor processor, Vertex vertex, int taskID);
}
