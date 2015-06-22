/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.api.processor;

import java.io.Serializable;

import static com.hazelcast.util.Preconditions.checkFalse;

import com.hazelcast.jet.impl.processor.descriptor.DefaultProcessorDescriptor;

public abstract class ProcessorDescriptor implements Serializable {
    private int taskCount = 1;

    public static ProcessorDescriptor create(Class<? extends ContainerProcessorFactory> clazz, Object... args) {
        return new DefaultProcessorDescriptor(clazz, args);
    }

    public int getTaskCount() {
        return this.taskCount;
    }

    public abstract Object[] getFactoryArgs();

    public abstract String getContainerProcessorFactoryClazz();

    public static Builder builder(Class<? extends ContainerProcessorFactory> clazz) {
        return new Builder(clazz);
    }

    public static Builder builder(Class<? extends ContainerProcessorFactory> clazz, Object... args) {
        return new Builder(clazz, args);
    }

    public static class Builder {
        private static final String MESSAGE = "ProcessorDescriptor has  already been built";

        private final ProcessorDescriptor processorDescriptor;
        private boolean build;

        public Builder(Class<? extends ContainerProcessorFactory> clazz, Object... args) {
            processorDescriptor = ProcessorDescriptor.create(clazz, args);
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
