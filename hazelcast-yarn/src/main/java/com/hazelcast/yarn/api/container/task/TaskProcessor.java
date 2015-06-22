package com.hazelcast.yarn.api.container.task;

import com.hazelcast.yarn.api.tuple.io.TupleInputStream;

public interface TaskProcessor {
    boolean process() throws Exception;

    boolean onChunk(TupleInputStream inputStream) throws Exception;

    boolean produced();

    boolean consumed();

    boolean isFinalized();

    boolean hasActiveConsumers();

    boolean hasActiveProducers();

    void reset();

    void onOpen();

    void onClose();

    void startFinalization();
}
