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

package com.hazelcast.jet.api.container;

import java.util.List;


import com.hazelcast.nio.Address;
import com.hazelcast.jet.api.dag.Edge;
import com.hazelcast.jet.api.dag.Vertex;
import com.hazelcast.jet.api.executor.Task;
import com.hazelcast.jet.api.data.DataWriter;
import com.hazelcast.jet.api.actor.ComposedActor;
import com.hazelcast.jet.api.actor.ObjectProducer;
import com.hazelcast.jet.impl.actor.shuffling.io.ShufflingSender;
import com.hazelcast.jet.impl.actor.shuffling.io.ShufflingReceiver;


public interface ContainerTask extends Task {
    void start(List<? extends ObjectProducer> producers);

    void interrupt();

    void markInvalidated();

    void destroy();

    void beforeExecution();

    void registerSinkWriters(List<DataWriter> sinkWriters);

    ComposedActor registerOutputChannel(DataChannel channel, Edge edge, DataContainer targetContainer);

    void handleProducerCompleted(ObjectProducer actor);

    void handleShufflingReceiverCompleted(ObjectProducer actor);

    void registerShufflingReceiver(Address member, ShufflingReceiver receiver);

    void registerShufflingSender(Address member, ShufflingSender sender);

    ShufflingReceiver getShufflingReceiver(Address endPoint);

    Vertex getVertex();

    void startFinalization();
}
