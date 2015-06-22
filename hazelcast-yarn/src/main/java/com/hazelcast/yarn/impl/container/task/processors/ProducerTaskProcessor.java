package com.hazelcast.yarn.impl.container.task.processors;


import com.hazelcast.yarn.impl.YarnUtil;
import com.hazelcast.yarn.api.tuple.Tuple;
import com.hazelcast.config.YarnApplicationConfig;
import com.hazelcast.yarn.api.actor.TupleProducer;
import com.hazelcast.yarn.api.tuple.io.TupleInputStream;
import com.hazelcast.yarn.api.container.ContainerContext;
import com.hazelcast.yarn.api.container.task.TaskProcessor;
import com.hazelcast.yarn.impl.tuple.io.DefaultTupleIOStream;
import com.hazelcast.yarn.api.processor.TupleContainerProcessor;

import static com.hazelcast.util.Preconditions.checkNotNull;

public class ProducerTaskProcessor implements TaskProcessor {
    protected final int taskID;
    private int nextProducerIdx = 0;
    protected boolean produced = false;
    protected boolean finalized = false;
    protected boolean finalizationStarted = false;
    protected boolean finalizationFinished = false;
    protected TupleProducer pendingProducer;


    protected final TupleProducer[] producers;
    protected final TupleContainerProcessor processor;
    protected final ContainerContext containerContext;
    protected final DefaultTupleIOStream tupleInputStream;
    protected final DefaultTupleIOStream tupleOutputStream;

    public ProducerTaskProcessor(TupleProducer[] producers,
                                 TupleContainerProcessor processor,
                                 ContainerContext containerContext,
                                 int taskID) {
        checkNotNull(processor);

        this.taskID = taskID;
        this.producers = producers;
        this.processor = processor;
        this.containerContext = containerContext;
        String applicationName = containerContext.getApplicationContext().getName();
        YarnApplicationConfig yarnApplicationConfig = containerContext.getNodeEngine().getConfig().getYarnApplicationConfig(applicationName);
        int tupleChunkSize = yarnApplicationConfig.getTupleChunkSize();
        this.tupleInputStream = new DefaultTupleIOStream(new Tuple[tupleChunkSize]);
        this.tupleOutputStream = new DefaultTupleIOStream(new Tuple[tupleChunkSize]);
    }

    public boolean onChunk(TupleInputStream inputStream) throws Exception {
        return true;
    }

    protected void checkFinalization() {
        if ((this.finalizationStarted) && (this.finalizationFinished)) {
            this.finalized = true;
            this.finalizationStarted = false;
            this.finalizationFinished = false;
            this.resetProducers();
        }
    }

    @Override
    public boolean process() throws Exception {
        int producersCount = this.producers.length;

        boolean produced = false;

        if (this.finalizationStarted) {
            this.finalizationFinished = this.processor.finalizeProcessor(
                    this.tupleOutputStream,
                    this.containerContext
            );

            return !this.processOutputStream();
        } else if (this.pendingProducer != null) {
            return this.processProducer(this.pendingProducer);
        }

        int lastIdx = 0;

        for (int i = this.nextProducerIdx; i < producersCount; i++) {
            lastIdx = i;
            TupleProducer producer = this.producers[i];

            Tuple[] inChunk = producer.produce();

            if ((YarnUtil.isEmpty(inChunk)) || (producer.lastProducedCount() <= 0)) {
                continue;
            }

            produced = true;

            this.tupleInputStream.consumeChunk(
                    inChunk,
                    producer.lastProducedCount()
            );

            if (!this.processProducer(producer)) {
                this.nextProducerIdx = (i + 1) % producersCount;
                return false;
            }
        }

        if (producersCount > 0) {
            this.nextProducerIdx = (lastIdx + 1) % producersCount;
            this.produced = produced;
        }

        return true;
    }

    private boolean processProducer(TupleProducer producer) throws Exception {
        if (!this.processor.process(
                this.tupleInputStream,
                this.tupleOutputStream,
                producer.getName(),
                this.containerContext
        )) {
            this.pendingProducer = producer;
        } else {
            this.pendingProducer = null;
        }

        if (!this.processOutputStream()) {
            this.produced = true;
            return false;
        }

        this.tupleOutputStream.reset();
        return this.pendingProducer == null;
    }


    private boolean processOutputStream() throws Exception {
        if (this.tupleOutputStream.size() == 0) {
            this.checkFinalization();
            return true;
        } else {
            if (!this.onChunk(this.tupleOutputStream)) {
                this.produced = true;
                return false;
            } else {
                this.checkFinalization();
                this.tupleOutputStream.reset();
                return true;
            }
        }
    }

    @Override
    public boolean produced() {
        return this.produced;
    }


    @Override
    public boolean isFinalized() {
        return this.finalized;
    }

    @Override
    public boolean hasActiveProducers() {
        return producers.length > 0;
    }

    @Override
    public void reset() {
        this.resetProducers();
        this.finalized = false;
        this.pendingProducer = null;
        this.finalizationStarted = false;
    }

    @Override
    public void onOpen() {
        for (TupleProducer producer : this.producers) {
            producer.open();
        }

        this.reset();
    }

    @Override
    public void onClose() {
        for (TupleProducer producer : this.producers) {
            producer.close();
        }
    }

    @Override
    public void startFinalization() {
        this.finalizationStarted = true;
    }

    private void resetProducers() {
        this.produced = false;
        this.nextProducerIdx = 0;
        this.tupleOutputStream.reset();
        this.tupleInputStream.reset();
    }

    @Override
    public boolean consumed() {
        return false;
    }

    @Override
    public boolean hasActiveConsumers() {
        return false;
    }
}
