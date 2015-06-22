package com.hazelcast.yarn.api.processor;

import com.hazelcast.yarn.api.dag.Vertex;

public interface TupleContainerProcessorFactory<KeyInput, ValueInput, KeyOutPut, ValueOutPut> {
    TupleContainerProcessor<KeyInput, ValueInput, KeyOutPut, ValueOutPut> getProcessor(Vertex vertex);
}
