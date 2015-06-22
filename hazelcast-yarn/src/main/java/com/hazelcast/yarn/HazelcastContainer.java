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

package com.hazelcast.yarn;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.ContainerId;

public class HazelcastContainer {
    private final int memory;
    private final NodeId nodeId;
    private final ContainerId id;
    private final int virtualCores;

    public HazelcastContainer(ContainerId id, NodeId nodeId,
                              int virtualCores, int memory) {
        this.id = id;
        this.nodeId = nodeId;
        this.virtualCores = virtualCores;
        this.memory = memory;
    }

    public int getMemory() {
        return this.memory;
    }

    public NodeId getNodeId() {
        return this.nodeId;
    }

    public ContainerId getId() {
        return this.id;
    }

    public int getVirtualCores() {
        return this.virtualCores;
    }
}
