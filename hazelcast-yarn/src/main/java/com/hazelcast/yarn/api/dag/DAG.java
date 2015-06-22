package com.hazelcast.yarn.api.dag;

import java.util.Iterator;
import java.util.Collection;

import com.hazelcast.nio.serialization.DataSerializable;

public interface DAG extends DataSerializable {
    DAG addVertex(Vertex vertex);

    Vertex getVertex(String vertexName);

    Collection<Vertex> getVertices();

    DAG addEdge(Edge edge);

    String getName();

    void validate() throws IllegalStateException;

    Iterator<Vertex> getTopologicalVertexIterator();

    Iterator<Vertex> getRevertedTopologicalVertexIterator();
}
