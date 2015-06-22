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

package com.hazelcast.jet.impl.dag.tap.sink;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.jet.api.data.tuple.Tuple;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.jet.impl.DefaultHashingStrategy;
import com.hazelcast.jet.impl.CalculationStrategyImpl;
import com.hazelcast.collection.impl.list.ListService;
import com.hazelcast.collection.impl.list.ListContainer;
import com.hazelcast.jet.api.container.ContainerContext;
import com.hazelcast.jet.api.data.io.ProducerInputStream;
import com.hazelcast.jet.api.dag.tap.SinkTapWriteStrategy;
import com.hazelcast.jet.api.strategy.CalculationStrategy;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;

public class HazelcastListPartitionWriter extends AbstractHazelcastWriter {
    private final ListContainer listContainer;
    private final CalculationStrategy calculationStrategy;

    public HazelcastListPartitionWriter(ContainerContext containerContext,
                                        SinkTapWriteStrategy sinkTapWriteStrategy,
                                        String name) {
        super(containerContext,
                getPartitionId(name, containerContext.getApplicationContext().getNodeEngine()),
                sinkTapWriteStrategy
        );
        NodeEngineImpl nodeEngine = (NodeEngineImpl) containerContext.getApplicationContext().getNodeEngine();
        ListService service = nodeEngine.getService(ListService.SERVICE_NAME);
        this.listContainer = service.getOrCreateContainer(name, false);
        this.calculationStrategy = new CalculationStrategyImpl(
                DefaultHashingStrategy.INSTANCE,
                getPartitionStrategy(),
                containerContext
        );
    }

    private static int getPartitionId(String name, NodeEngine nodeEngine) {
        Data data = nodeEngine.getSerializationService().toData(name, StringPartitioningStrategy.INSTANCE);
        return nodeEngine.getPartitionService().getPartitionId(data);
    }

    @Override
    protected void processChunk(ProducerInputStream<Object> chunk) {
        for (int i = 0; i < chunk.size(); i++) {
            Tuple tuple = (Tuple) chunk.get(i);

            if (tuple == null) {
                continue;
            }

            if (!this.listContainer.hasEnoughCapacity(chunk.size())) {
                throw new IllegalStateException("IList " + getName() + " capacity exceeded");
            }

            if (!(tuple.getKey(0) instanceof Number)) {
                throw new IllegalStateException("Key for IList tuple should be Integer");
            }

            this.listContainer.add(tuple.getValueData(this.calculationStrategy, getNodeEngine()));
        }
    }

    @Override
    protected void onOpen() {
        if (getSinkTapWriteStrategy() == SinkTapWriteStrategy.CLEAR_AND_REPLACE) {
            this.listContainer.clear();
        }
    }

    @Override
    public PartitioningStrategy getPartitionStrategy() {
        return StringPartitioningStrategy.INSTANCE;
    }

    @Override
    public boolean isPartitioned() {
        return false;
    }
}
