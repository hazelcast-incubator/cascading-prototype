package com.hazelcast.yarn.impl.dag;


import java.util.Stack;
import java.util.Iterator;

import com.hazelcast.yarn.api.dag.Vertex;

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
