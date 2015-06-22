package com.hazelcast.yarn.impl.dag;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

import com.hazelcast.yarn.api.dag.Edge;
import com.hazelcast.yarn.api.dag.Vertex;
import com.hazelcast.yarn.api.tap.SinkTap;
import com.hazelcast.yarn.api.tap.SinkTapWriteStrategy;
import com.hazelcast.yarn.api.tap.TapType;
import com.hazelcast.yarn.api.tap.SourceTap;
import com.hazelcast.yarn.impl.tap.sink.HazelcastSinkTap;
import com.hazelcast.yarn.api.processor.ProcessorDescriptor;
import com.hazelcast.yarn.impl.tap.source.HazelcastSourceTap;

import static com.hazelcast.util.Preconditions.checkNotNull;

public class VertexImpl implements Vertex {
    private final String name;
    private final ProcessorDescriptor descriptor;

    private final List<Edge> inputEdges = new ArrayList<Edge>();
    private final List<Edge> outputEdges = new ArrayList<Edge>();
    private final List<SinkTap> sinks = new ArrayList<SinkTap>();
    private final List<Vertex> inputVertices = new ArrayList<Vertex>();
    private final List<Vertex> outputVertices = new ArrayList<Vertex>();
    private final List<SourceTap> sources = new ArrayList<SourceTap>();

    public VertexImpl(String name,
                      ProcessorDescriptor descriptor) {
        checkNotNull(descriptor);
        checkNotNull(name);

        this.name = name;
        this.descriptor = descriptor;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public void addSourceTap(SourceTap sourceTap) {
        this.sources.add(sourceTap);
    }

    @Override
    public void addSinkTap(SinkTap sinkTap) {
        this.sinks.add(sinkTap);
    }

    @Override
    public void addSourceList(String name) {
        this.sources.add(new HazelcastSourceTap(name, TapType.HAZELCAST_LIST));
    }

    @Override
    public void addSourceMap(String name) {
        this.sources.add(new HazelcastSourceTap(name, TapType.HAZELCAST_MAP));
    }

    @Override
    public void addSourceMultiMap(String name) {
        this.sources.add(new HazelcastSourceTap(name, TapType.HAZELCAST_MULTIMAP));
    }

    @Override
    public void addSinkList(String name) {
        this.sinks.add(new HazelcastSinkTap(name, TapType.HAZELCAST_LIST));
    }

    @Override
    public void addSinkList(String name, SinkTapWriteStrategy sinkTapWriteStrategy) {
        this.sinks.add(new HazelcastSinkTap(name, TapType.HAZELCAST_LIST, sinkTapWriteStrategy));
    }

    @Override
    public void addSinkMap(String name) {
        this.sinks.add(new HazelcastSinkTap(name, TapType.HAZELCAST_MAP));
    }

    @Override
    public void addSinkMap(String name, SinkTapWriteStrategy sinkTapWriteStrategy) {
        this.sinks.add(new HazelcastSinkTap(name, TapType.HAZELCAST_MAP, sinkTapWriteStrategy));
    }

    @Override
    public void addSinkMultiMap(String name) {
        this.sinks.add(new HazelcastSinkTap(name, TapType.HAZELCAST_MULTIMAP));
    }

    @Override
    public void addSinkMultiMap(String name, SinkTapWriteStrategy sinkTapWriteStrategy) {
        this.sinks.add(new HazelcastSinkTap(name, TapType.HAZELCAST_MULTIMAP, sinkTapWriteStrategy));
    }

    @Override
    public void addSourceFile(String name) {
        this.sources.add(new HazelcastSourceTap(name, TapType.FILE));
    }

    @Override
    public void addSinkFile(String name, SinkTapWriteStrategy sinkTapWriteStrategy) {
        sinks.add(new HazelcastSinkTap(name, TapType.FILE, sinkTapWriteStrategy));
    }

    @Override
    public void addSinkFile(String name) {
        sinks.add(new HazelcastSinkTap(name, TapType.FILE));
    }

    @Override
    public void addSourceHDFile(String name) {
        this.sources.add(new HazelcastSourceTap(name, TapType.HD_FILE));
    }

    @Override
    public void addSinkHDFile(String name) {
        sinks.add(new HazelcastSinkTap(name, TapType.HD_FILE));
    }

    @Override
    public void addOutputVertex(Vertex outputVertex, Edge edge) {
        this.outputVertices.add(outputVertex);
        this.outputEdges.add(edge);
    }

    @Override
    public void addInputVertex(Vertex inputVertex, Edge edge) {
        this.inputVertices.add(inputVertex);
        this.inputEdges.add(edge);
    }

    @Override
    public List<Edge> getInputEdges() {
        return Collections.unmodifiableList(this.inputEdges);
    }

    @Override
    public List<Edge> getOutputEdges() {
        return Collections.unmodifiableList(this.outputEdges);
    }

    @Override
    public List<Vertex> getInputVertices() {
        return Collections.unmodifiableList(this.inputVertices);
    }

    @Override
    public List<Vertex> getOutputVertices() {
        return Collections.unmodifiableList(this.outputVertices);
    }

    @Override
    public List<SourceTap> getSources() {
        return Collections.unmodifiableList(this.sources);
    }

    @Override
    public List<SinkTap> getSinks() {
        return Collections.unmodifiableList(this.sinks);
    }

    @Override
    public ProcessorDescriptor getDescriptor() {
        return this.descriptor;
    }

    @Override
    public boolean hasOutputShuffler() {
        for (Edge edge : this.outputEdges) {
            if (edge.isShuffled())
                return true;
        }

        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VertexImpl vertex = (VertexImpl) o;
        return !(this.name != null ? !this.name.equals(vertex.name) : vertex.name != null);
    }

    @Override
    public int hashCode() {
        return this.name != null ? this.name.hashCode() : 0;
    }
}
