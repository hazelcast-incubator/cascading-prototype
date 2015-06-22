package com.hazelcast.yarn.impl.container.task.processors;

import com.hazelcast.yarn.api.actor.TupleConsumer;
import com.hazelcast.yarn.api.tuple.io.TupleInputStream;

public class ConsumersProcessor {
    private boolean consumed;
    private TupleInputStream inputStream;
    private final TupleConsumer[] consumers;

    public ConsumersProcessor(TupleConsumer[] consumers) {
        this.consumers = consumers;
    }

    public boolean process(TupleInputStream inputStream) throws Exception {
        boolean success = true;
        boolean consumed = false;

        if (this.inputStream == null) {
            this.inputStream = inputStream;

            for (TupleConsumer consumer : this.consumers) {
                consumer.consumeChunk(inputStream);
                success = success && consumer.isFlushed();
                consumed = consumed || consumer.lastConsumedCount() > 0;
            }
        } else {
            for (TupleConsumer consumer : this.consumers) {
                success = success & consumer.isFlushed();
                consumed = consumed || consumer.lastConsumedCount() > 0;
            }
        }

        if (success) {
            this.inputStream = null;
        }

        this.consumed = consumed;
        return success;
    }

    public boolean isConsumed() {
        return this.consumed;
    }

    public void reset() {
        this.consumed = false;
        this.inputStream = null;
    }
}