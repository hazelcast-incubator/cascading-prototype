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

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.jet.api.data.DataWriter;
import com.hazelcast.jet.api.container.ContainerContext;

public abstract class SinkTap implements Tap {
    public abstract DataWriter[] getWriters(NodeEngine nodeEngine, ContainerContext containerContext);

    public boolean isSource() {
        return false;
    }

    public boolean isSink() {
        return true;
    }

    public abstract SinkTapWriteStrategy getTapStrategy();

    public abstract SinkOutputStream getSinkOutputStream();

    public boolean isPartitioned() {
        return true;
    }
}
