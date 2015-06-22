package com.hazelcast.yarn.impl.container.task.processors;

import com.hazelcast.yarn.api.actor.TupleProducer;
import com.hazelcast.yarn.api.tuple.io.TupleInputStream;
import com.hazelcast.yarn.api.container.ContainerContext;
import com.hazelcast.yarn.api.container.task.TaskProcessor;
import com.hazelcast.yarn.api.processor.TupleContainerProcessor;

public class ActorTaskProcessor extends ProducerTaskProcessor {
    private final TaskProcessor consumerProcessor;
    protected boolean consumed;

    public ActorTaskProcessor(TupleProducer[] producers,
                              TupleContainerProcessor processor,
                              ContainerContext containerContext,
                              TaskProcessor consumerProcessor,
                              int taskID) {
        super(producers, processor, containerContext, taskID);
        this.consumerProcessor = consumerProcessor;
    }

    public boolean onChunk(TupleInputStream inputStream) throws Exception {
        boolean success = this.consumerProcessor.onChunk(inputStream);
        this.consumed = this.consumerProcessor.consumed();
        return success;
    }

    public boolean process() throws Exception {
        if (this.tupleOutputStream.size() == 0) {
            boolean result = super.process();

            if (!this.produced) {
                this.consumed = false;
            }

            return result;
        } else {
            boolean success = onChunk(this.tupleOutputStream);

            if (success) {
                this.checkFinalization();
                this.tupleOutputStream.reset();
            }

            return success;
        }
    }

    @Override
    public boolean consumed() {
        return consumed;
    }

    @Override
    public boolean hasActiveConsumers() {
        return this.consumerProcessor.hasActiveConsumers();
    }

    @Override
    public void reset() {
        super.reset();
        this.consumerProcessor.reset();
    }

    @Override
    public void onOpen() {
        super.onOpen();
        this.consumerProcessor.onOpen();
        this.reset();
    }

    @Override
    public void onClose() {
        super.onClose();
        this.consumerProcessor.onClose();
    }

    public void startFinalization() {
        super.startFinalization();
        this.consumerProcessor.startFinalization();
    }
}
