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

package com.hazelcast.jet.api.data.tuple;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.jet.api.PartitionIdAware;
import com.hazelcast.jet.api.strategy.CalculationStrategy;
import com.hazelcast.jet.api.strategy.CalculationStrategyAware;
import com.hazelcast.nio.serialization.DataSerializable;

public interface Tuple<K, V> extends CalculationStrategyAware, PartitionIdAware, DataSerializable {
    Data getKeyData(NodeEngine nodeEngine);

    Data getValueData(NodeEngine nodeEngine);

    Data getKeyData(CalculationStrategy partitioningStrategy, NodeEngine nodeEngine);

    Data getValueData(CalculationStrategy partitioningStrategy, NodeEngine nodeEngine);

    Data getKeyData(int index, CalculationStrategy partitioningStrategy, NodeEngine nodeEngine);

    Data getValueData(int index, CalculationStrategy partitioningStrategy, NodeEngine nodeEngine);

    K[] cloneKeys();

    V[] cloneValues();

    K getKey(int index);

    V getValue(int index);

    int keySize();

    int valueSize();

    void setKey(int index, K value);

    void setValue(int index, V value);
}
