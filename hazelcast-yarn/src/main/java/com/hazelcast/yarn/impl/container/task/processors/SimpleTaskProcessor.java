package com.hazelcast.yarn.impl.container.task.processors;

import com.hazelcast.yarn.api.tuple.Tuple;
import com.hazelcast.yarn.api.tuple.io.TupleInputStream;
import com.hazelcast.yarn.api.container.ContainerContext;
import com.hazelcast.yarn.api.container.task.TaskProcessor;
import com.hazelcast.yarn.impl.tuple.io.DefaultTupleIOStream;
import com.hazelcast.yarn.api.processor.TupleContainerProcessor;

import static com.hazelcast.util.Preconditions.checkNotNull;

public class SimpleTaskProcessor implements TaskProcessor {
    private static final Tuple[] DUMMY_CHUNK = new Tuple[0];

    private boolean finalized = false;

    private final int taskID;
    protected boolean finalizationStarted = false;
    private final TupleContainerProcessor processor;
    private final ContainerContext containerContext;
    private final DefaultTupleIOStream tupleInputStream;
    private final DefaultTupleIOStream tupleOutputStream;


    public SimpleTaskProcessor(TupleContainerProcessor processor, ContainerContext containerContext, int taskID) {
        checkNotNull(processor);
        this.taskID = taskID;
        this.processor = processor;
        this.containerContext = containerContext;
        this.tupleInputStream = new DefaultTupleIOStream(DUMMY_CHUNK);
        this.tupleOutputStream = new DefaultTupleIOStream(DUMMY_CHUNK);
    }

    @Override
    public boolean process() throws Exception {
        if (!this.finalizationStarted) {
            this.processor.process(
                    this.tupleInputStream,
                    this.tupleOutputStream,
                    null,
                    this.containerContext
            );

            this.finalizationStarted = true;
            return true;
        } else {
            this.finalized = this.processor.finalizeProcessor(
                    this.tupleOutputStream,
                    this.containerContext
            );

            return true;
        }
    }

    @Override
    public boolean isFinalized() {
        return this.finalized;
    }

    @Override
    public boolean hasActiveConsumers() {
        return false;
    }

    @Override
    public boolean hasActiveProducers() {
        return false;
    }

    @Override
    public void reset() {
        this.finalized = false;
        this.tupleInputStream.reset();
        this.tupleOutputStream.reset();
        this.finalizationStarted = false;
    }

    @Override
    public void startFinalization() {
        this.finalizationStarted = true;
    }

    @Override
    public boolean onChunk(TupleInputStream tupleOutputStream) throws Exception {
        return true;
    }

    @Override
    public boolean produced() {
        return false;
    }

    @Override
    public boolean consumed() {
        return false;
    }

    @Override
    public void onOpen() {
        this.reset();
    }

    @Override
    public void onClose() {

    }
}
