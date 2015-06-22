package com.hazelcast.yarn.impl.processor.descriptor;

import com.hazelcast.yarn.api.processor.ProcessorDescriptor;
import com.hazelcast.yarn.api.processor.TupleContainerProcessorFactory;

public class DefaultProcessorDescriptor extends ProcessorDescriptor {
    private final String clazz;

    public DefaultProcessorDescriptor(Class<? extends TupleContainerProcessorFactory> clazz) {
        this.clazz = clazz.getName();
    }

    @Override
    public String getContainerProcessorClazz() {
        return clazz;
    }
}
