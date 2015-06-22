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

package com.hazelcast.jet.impl.dag;


import java.util.Stack;
import java.util.Iterator;

import com.hazelcast.jet.api.dag.Vertex;

import static com.hazelcast.util.Preconditions.checkNotNull;

public class TopologicalOrderIterator implements Iterator<Vertex> {
    private final Stack<Vertex> topologicalVertexStack;

    public TopologicalOrderIterator(Stack<Vertex> topologicalVertexStack) {
        checkNotNull(topologicalVertexStack, "topologicalVertexStack can't be null");
        this.topologicalVertexStack = topologicalVertexStack;
    }

    @Override
    public boolean hasNext() {
        return !topologicalVertexStack.isEmpty();
    }

    @Override
    public Vertex next() {
        return topologicalVertexStack.pop();
    }

    @Override
    public void remove() {
        throw new IllegalStateException("Not supported");
    }
}
