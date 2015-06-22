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

package com.hazelcast.jet.impl.dag.tap.source;

import com.hazelcast.jet.api.dag.Vertex;
import com.hazelcast.jet.api.dag.tap.TapType;
import com.hazelcast.jet.api.data.DataReader;
import com.hazelcast.jet.api.data.tuple.TupleFactory;
import com.hazelcast.jet.api.container.ContainerContext;

public final class HazelcastReaderFactory {
    private HazelcastReaderFactory() {

    }

    public static <K, V> DataReader getReader(TapType tapType,
                                              String name,
                                              ContainerContext containerContext,
                                              int partitionId,
                                              TupleFactory tupleFactory,
                                              Vertex vertex) {
        switch (tapType) {
            case HAZELCAST_LIST:
                return new HazelcastListPartitionReader<K, V>(containerContext, name, tupleFactory, vertex);
            case HAZELCAST_MAP:
                return new HazelcastMapPartitionReader<K, V>(containerContext, name, partitionId, tupleFactory, vertex);
            case HAZELCAST_MULTIMAP:
                return new HazelcastMultiMapPartitionReader<K, V>(containerContext, name, partitionId, tupleFactory, vertex);
            default:
                throw new IllegalStateException("Unknown tuple type: " + tapType);
        }
    }


    public static DataReader getReader(TapType tapType,
                                       String name,
                                       ContainerContext applicationContext,
                                       int partitionId,
                                       long start,
                                       long end,
                                       TupleFactory tupleFactory,
                                       Vertex vertex) {
        switch (tapType) {
            case FILE:
                return new DataFileReader(applicationContext, vertex, partitionId, tupleFactory, name, start, end);
            default:
                throw new IllegalStateException("Unknown tuple type: " + tapType);
        }
    }
}
