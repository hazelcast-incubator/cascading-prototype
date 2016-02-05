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

package com.hazelcast.jet.spi.dag;

import java.util.Iterator;
import java.util.Collection;

import com.hazelcast.nio.serialization.DataSerializable;

/**
 * Direct acyclic graph representation;
 * <p/>
 * DAG describes topology of calculation flow;
 * <p/>
 * <pre>
 *
 *     Vertex1 -> Vertex2  -> Vertex3
 *                         -> Vertex4
 * </pre>
 * <p/>
 * Data will be passed from vertex to vertex
 */
public interface DAG extends DataSerializable {
    /**
     * @param vertex - vertex of the DAG;
     * @return - DAG itself;
     */
    DAG addVertex(Vertex vertex);

    /**
     * Return vertex with corresponding name;
     *
     * @param vertexName - name of the vertex;
     * @return - corresponding vertex;
     */
    Vertex getVertex(String vertexName);

    /**
     * @return - collections of DAG's vertices;
     */
    Collection<Vertex> getVertices();

    /**
     * Add edge to dag;
     *
     * @param edge - corresponding edge;
     * @return -  DAG itself;
     */
    DAG addEdge(Edge edge);

    /**
     * @return - name of the DAG;
     */
    String getName();

    /**
     * @param vertex - some vertex;
     * @return - true if DAG contains vertex, false - otherwise;
     */
    boolean containsVertex(Vertex vertex);

    /**
     * @param edge - some edge;
     * @return - true if DAG contains edge, false - otherwise;
     */
    boolean containsEdge(Edge edge);

    /**
     * Validate DAG's consistency;
     * <p/>
     * It checks:
     * <p/>
     * <pre>
     *      -   duplicate of vertices names;
     *      -   duplicate of edges names;
     *      -   absence of loops on DAG;
     * </pre>
     *
     * @throws IllegalStateException
     */
    void validate() throws IllegalStateException;

    /**
     * @return - iterator over DAG's vertices on accordance with DAG's topology;
     */
    Iterator<Vertex> getTopologicalVertexIterator();

    /**
     * @return - iterator over DAG's vertices on accordance with DAG's reverted topology;
     */
    Iterator<Vertex> getRevertedTopologicalVertexIterator();
}
