package com.hazelcast.yarn.api.processor;

import java.io.Serializable;

import static com.hazelcast.util.Preconditions.checkFalse;

import com.hazelcast.yarn.impl.processor.descriptor.DefaultProcessorDescriptor;

public abstract class ProcessorDescriptor implements Serializable {
    private int taskCount = 1;

    public static ProcessorDescriptor create(Class<? extends TupleContainerProcessorFactory> clazz) {
        return new DefaultProcessorDescriptor(clazz);
    }

    public int getTaskCount() {
        return this.taskCount;
    }

    public abstract String getContainerProcessorClazz();

    public static Builder builder(Class<? extends TupleContainerProcessorFactory> clazz) {
        return new Builder(clazz);
    }

    public static class Builder {
        private final ProcessorDescriptor processorDescriptor;
        private boolean build = false;
        private static final String MESSAGE = "ProcessorDescriptor has  already been built";

        public Builder(Class<? extends TupleContainerProcessorFactory> clazz) {
            processorDescriptor = ProcessorDescriptor.create(clazz);
        }

        public Builder withTaskCount(int taskCount) {
            checkFalse(build, MESSAGE);
            processorDescriptor.taskCount = taskCount;
            return this;
        }

        public ProcessorDescriptor build() {
            checkFalse(build, MESSAGE);
            build = true;
            return processorDescriptor;
        }
    }
}
