package com.hazelcast.yarn.impl.container.task.processors;

import com.hazelcast.yarn.api.tuple.Tuple;

import com.hazelcast.config.YarnApplicationConfig;
import com.hazelcast.yarn.api.actor.TupleConsumer;
import com.hazelcast.yarn.api.tuple.io.TupleInputStream;
import com.hazelcast.yarn.api.container.ContainerContext;
import com.hazelcast.yarn.api.container.task.TaskProcessor;
import com.hazelcast.yarn.impl.tuple.io.DefaultTupleIOStream;
import com.hazelcast.yarn.api.processor.TupleContainerProcessor;

import static com.hazelcast.util.Preconditions.checkNotNull;

public class ConsumerTaskProcessor implements TaskProcessor {
    protected static final Tuple[] DUMMY_CHUNK = new Tuple[0];

    protected boolean consumed = false;

    protected boolean finalized = false;

    protected final TupleConsumer[] consumers;

    protected boolean finalizationFinished = false;

    protected boolean finalizationStarted = false;

    protected final TupleContainerProcessor processor;

    protected final ContainerContext containerContext;

    private final ConsumersProcessor consumersProcessor;

    protected final DefaultTupleIOStream tupleInputStream;

    protected final DefaultTupleIOStream tupleOutputStream;


    public ConsumerTaskProcessor(TupleConsumer[] consumers,
                                 TupleContainerProcessor processor,
                                 ContainerContext containerContext) {
        checkNotNull(consumers);
        checkNotNull(processor);
        checkNotNull(containerContext);

        this.consumers = consumers;
        this.processor = processor;
        this.containerContext = containerContext;
        this.consumersProcessor = new ConsumersProcessor(consumers);
        this.tupleInputStream = new DefaultTupleIOStream(DUMMY_CHUNK);
        String applicationName = containerContext.getApplicationContext().getName();
        YarnApplicationConfig yarnApplicationConfig = containerContext.getNodeEngine().getConfig().getYarnApplicationConfig(applicationName);
        int tupleChunkSize = yarnApplicationConfig.getTupleChunkSize();
        this.tupleOutputStream = new DefaultTupleIOStream(new Tuple[tupleChunkSize]);
        this.reset();
    }

    private void checkFinalization() {
        if ((this.finalizationStarted) && (this.finalizationFinished)) {
            this.finalized = true;
            this.finalizationStarted = false;
            this.finalizationFinished = false;
            this.resetConsumers();
        }
    }

    @Override
    public boolean process() throws Exception {
        if (this.tupleOutputStream.size() > 0) {
            boolean success = this.onChunk(this.tupleOutputStream);

            if (success) {
                this.tupleOutputStream.reset();
                this.checkFinalization();
            }

            return success;
        } else {
            if (!this.finalizationStarted) {
                this.processor.process(
                        this.tupleInputStream,
                        this.tupleOutputStream,
                        null,
                        this.containerContext
                );
            } else {
                this.finalizationFinished = this.processor.finalizeProcessor(this.tupleOutputStream, this.containerContext);
            }

            if (this.tupleOutputStream.size() > 0) {
                boolean success = this.onChunk(this.tupleOutputStream);

                if (success) {
                    this.tupleOutputStream.reset();
                }

                return success;
            } else {
                this.checkFinalization();
            }

            return true;
        }
    }

    @Override
    public boolean onChunk(TupleInputStream inputStream) throws Exception {
        this.consumed = false;

        if (inputStream.size() > 0) {
            boolean success = this.consumersProcessor.process(inputStream);
            boolean consumed = this.consumersProcessor.isConsumed();

            if (success) {
                this.resetConsumers();
            }

            this.consumed = consumed;
            return success;
        } else {
            return true;
        }
    }

    @Override
    public boolean consumed() {
        return this.consumed;
    }

    @Override
    public boolean isFinalized() {
        return this.finalized;
    }

    @Override
    public void reset() {
        this.resetConsumers();

        this.finalizationStarted = false;
        this.finalizationFinished = false;
        this.finalized = false;
    }

    private void resetConsumers() {
        this.consumed = false;
        this.tupleOutputStream.reset();
        this.consumersProcessor.reset();
    }

    public void onOpen() {
        for (TupleConsumer consumer : consumers) {
            consumer.open();
        }

        this.reset();
    }

    @Override
    public void onClose() {
        this.reset();

        for (TupleConsumer consumer : consumers) {
            consumer.close();
        }
    }

    @Override
    public void startFinalization() {
        this.finalizationStarted = true;
    }

    @Override
    public boolean produced() {
        return false;
    }

    @Override
    public boolean hasActiveConsumers() {
        return true;
    }

    @Override
    public boolean hasActiveProducers() {
        return false;
    }
}