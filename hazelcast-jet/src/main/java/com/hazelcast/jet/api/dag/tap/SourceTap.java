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

package com.hazelcast.jet.api.dag.tap;

import com.hazelcast.jet.api.dag.Vertex;
import com.hazelcast.jet.api.data.DataReader;
import com.hazelcast.jet.api.data.tuple.TupleFactory;
import com.hazelcast.jet.api.container.ContainerContext;

public abstract class SourceTap implements Tap {
    public abstract DataReader[] getReaders(ContainerContext containerContext,
                                              Vertex vertex,
                                              TupleFactory tupleFactory);

    public boolean isSource() {
        return true;
    }

    public boolean isSink() {
        return false;
    }
}
