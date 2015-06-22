package com.hazelcast.yarn.api.dag;

import java.util.List;
import java.io.Serializable;

import com.hazelcast.yarn.api.tap.SinkTap;
import com.hazelcast.yarn.api.tap.SourceTap;
import com.hazelcast.yarn.api.tap.SinkTapWriteStrategy;
import com.hazelcast.yarn.api.processor.ProcessorDescriptor;

public interface Vertex extends Serializable {
    void addSourceTap(SourceTap sourceTap);

    void addSinkTap(SinkTap sinkTap);

    void addSourceList(String name);

    void addSourceMap(String name);

    void addSourceMultiMap(String name);

    void addSinkList(String name);

    void addSinkList(String name, SinkTapWriteStrategy sinkTapWriteStrategy);

    void addSinkMap(String name);

    void addSinkMap(String name, SinkTapWriteStrategy sinkTapWriteStrategy);

    void addSinkMultiMap(String name);

    void addSinkMultiMap(String name, SinkTapWriteStrategy sinkTapWriteStrategy);

    void addSourceFile(String name);

    void addSinkFile(String name);
    
    void addSinkFile(String name, SinkTapWriteStrategy sinkTapWriteStrategy);

    void addSourceHDFile(String name);

    void addSinkHDFile(String name);

    void addOutputVertex(Vertex outputVertex, Edge edge);

    void addInputVertex(Vertex inputVertex, Edge edge);

    String getName();

    List<Edge> getInputEdges();

    List<Edge> getOutputEdges();

    List<Vertex> getInputVertices();

    List<Vertex> getOutputVertices();

    List<SourceTap> getSources();

    List<SinkTap> getSinks();

    ProcessorDescriptor getDescriptor();

    boolean hasOutputShuffler();
}
