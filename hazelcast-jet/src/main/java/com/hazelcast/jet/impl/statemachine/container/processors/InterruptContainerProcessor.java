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

package com.hazelcast.jet.impl.statemachine.container.processors;

import com.hazelcast.jet.api.Dummy;
import com.hazelcast.jet.api.container.ContainerTask;
import com.hazelcast.jet.api.container.DataContainer;
import com.hazelcast.jet.api.container.ContainerPayLoadProcessor;

public class InterruptContainerProcessor implements ContainerPayLoadProcessor<Dummy> {
    private final DataContainer container;

    public InterruptContainerProcessor(DataContainer container) {
        this.container = container;
    }

    @Override
    public void process(Dummy payload) throws Exception {
        for (ContainerTask containerTask : this.container.getContainerTasks()) {
            containerTask.interrupt();
        }
    }
}
