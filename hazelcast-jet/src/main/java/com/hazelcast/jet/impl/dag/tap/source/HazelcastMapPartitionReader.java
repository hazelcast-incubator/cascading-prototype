/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.dag.tap.source;


import com.hazelcast.config.MapConfig;

import com.hazelcast.jet.spi.dag.Vertex;

import com.hazelcast.map.impl.MapService;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.jet.spi.data.tuple.Tuple;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.jet.impl.strategy.DefaultHashingStrategy;
import com.hazelcast.jet.spi.data.tuple.TupleFactory;
import com.hazelcast.jet.impl.strategy.CalculationStrategyImpl;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.jet.impl.data.tuple.TupleIterator;
import com.hazelcast.jet.spi.data.tuple.TupleConvertor;
import com.hazelcast.jet.spi.strategy.CalculationStrategy;
import com.hazelcast.jet.spi.container.ContainerDescriptor;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.jet.impl.actor.ByReferenceDataTransferringStrategy;

public class HazelcastMapPartitionReader<K, V> extends AbstractHazelcastReader<Tuple<K, V>> {
    private final MapConfig mapConfig;
    private final CalculationStrategy calculationStrategy;

    private final TupleConvertor<Record, K, V> tupleConverter = new TupleConvertor<Record, K, V>() {
        @Override
        public Tuple<K, V> convert(Record record, SerializationService ss) {
            Object value;

            if (mapConfig.getInMemoryFormat() == InMemoryFormat.BINARY) {
                value = ss.toObject(record.getValue());
            } else {
                value = record.getValue();
            }

            return tupleFactory.tuple(
                    ss.<K>toObject(record.getKey()),
                    (V) value,
                    getPartitionId(),
                    calculationStrategy
            );
        }
    };

    public HazelcastMapPartitionReader(ContainerDescriptor containerDescriptor,
                                       String name,
                                       int partitionId,
                                       TupleFactory tupleFactory,
                                       Vertex vertex) {
        super(containerDescriptor, name, partitionId, tupleFactory, vertex, ByReferenceDataTransferringStrategy.INSTANCE);
        NodeEngineImpl nodeEngine = (NodeEngineImpl) containerDescriptor.getNodeEngine();

        this.mapConfig = nodeEngine.getConfig().getMapConfig(name);
        MapService service = nodeEngine.getService(MapService.SERVICE_NAME);
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        MapContainer mapContainer = mapServiceContext.getMapContainer(name);
        PartitioningStrategy partitioningStrategy = mapContainer.getPartitioningStrategy();
        partitioningStrategy = partitioningStrategy == null ? StringPartitioningStrategy.INSTANCE : partitioningStrategy;

        this.calculationStrategy = new CalculationStrategyImpl(
                DefaultHashingStrategy.INSTANCE,
                partitioningStrategy,
                this.containerDescriptor
        );
    }

    @Override
    protected void onClose() {

    }

    @Override
    public void onOpen() {
        NodeEngineImpl nei = (NodeEngineImpl) this.nodeEngine;
        SerializationService ss = nei.getSerializationService();
        MapService mapService = nei.getService(MapService.SERVICE_NAME);
        PartitionContainer partitionContainer = mapService.getMapServiceContext().getPartitionContainer(getPartitionId());
        RecordStore recordStore = partitionContainer.getRecordStore(getName());
        this.iterator = new TupleIterator<Record, K, V>(recordStore.iterator(), tupleConverter, ss);
    }

    @Override
    public boolean readFromPartitionThread() {
        return true;
    }
}
