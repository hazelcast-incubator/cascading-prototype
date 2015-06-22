package com.hazelcast.yarn.impl.dag;

import com.hazelcast.yarn.api.dag.Edge;
import com.hazelcast.yarn.api.dag.Vertex;
import com.hazelcast.yarn.api.ShufflingStrategy;

public class EdgeImpl implements Edge {
    private final String name;
    private final Vertex from;
    private final Vertex to;
    private final boolean shuffled;
    private final ShufflingStrategy shufflingStrategy;

    public EdgeImpl(String name, Vertex from, Vertex to) {
        this(name, from, to, false);
    }

    public EdgeImpl(String name, Vertex from, Vertex to, boolean shuffled) {
        this(name, from, to, shuffled, null);
    }

    public EdgeImpl(String name, Vertex from, Vertex to, boolean shuffled, ShufflingStrategy shufflingStrategy) {
        this.to = to;
        this.name = name;
        this.from = from;
        this.shuffled = shuffled;
        this.shufflingStrategy = shufflingStrategy;
    }

    @Override
    public Vertex getOutputVertex() {
        return this.to;
    }

    @Override
    public boolean isShuffled() {
        return shuffled;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Vertex getInputVertex() {
        return this.from;
    }

    @Override
    public ShufflingStrategy getShufflingStrategy() {
        return this.shufflingStrategy;
    }
}
