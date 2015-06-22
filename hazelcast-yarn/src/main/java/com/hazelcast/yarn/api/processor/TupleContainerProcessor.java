package com.hazelcast.yarn.api.processor;

import com.hazelcast.yarn.api.tuple.io.TupleInputStream;
import com.hazelcast.yarn.api.container.ContainerContext;
import com.hazelcast.yarn.api.tuple.io.TupleOutputStream;

public interface TupleContainerProcessor<KeyInput, ValueInput, KeyOutPut, ValueOutPut> extends ContainerProcessor {
    void beforeProcessing(ContainerContext containerContext);

    boolean process(TupleInputStream<KeyInput, ValueInput> inputStream,
                    TupleOutputStream<KeyOutPut, ValueOutPut> outputStream,
                    String sourceName,
                    ContainerContext containerContext
    ) throws Exception;

    boolean finalizeProcessor(TupleOutputStream<KeyOutPut, ValueOutPut> outputStream, ContainerContext containerContext) throws Exception;
}
