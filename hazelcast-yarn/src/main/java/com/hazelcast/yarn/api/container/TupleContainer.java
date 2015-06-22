package com.hazelcast.yarn.api.container;

import java.util.Map;
import java.util.List;

import com.hazelcast.yarn.api.dag.Vertex;
import com.hazelcast.yarn.api.container.task.TaskEvent;
import com.hazelcast.yarn.api.processor.ProcessorDescriptor;
import com.hazelcast.yarn.api.processor.TupleContainerProcessorFactory;
import com.hazelcast.yarn.api.statemachine.container.tuplecontainer.TupleContainerState;
import com.hazelcast.yarn.api.statemachine.container.tuplecontainer.TupleContainerEvent;
import com.hazelcast.yarn.api.statemachine.container.tuplecontainer.TupleContainerResponse;

public interface TupleContainer extends Container<TupleContainerEvent, TupleContainerState, TupleContainerResponse> {
    TupleContainerProcessorFactory getContainerProcessorFactory();

    ProcessorDescriptor getProcessorDescriptor();

    List<TupleChannel> getInputChannels();

    List<TupleChannel> getOutputChannels();

    void addInputChannel(TupleChannel channel);

    void addOutputChannel(TupleChannel channel);

    void handleTaskEvent(ContainerTask containerTask, TaskEvent event);

    void handleTaskEvent(ContainerTask containerTask, TaskEvent event, Throwable error);

    Map<Integer, ContainerTask> getTasksCache();

    ContainerTask[] getContainerTasks();

    Vertex getVertex();

    void start() throws Exception;

    void execute() throws Exception;

    void destroy() throws Exception;

    void invalidate();
}
