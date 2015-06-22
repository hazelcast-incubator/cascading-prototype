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

package com.hazelcast.jet.api.dag;

import java.util.List;

import com.hazelcast.jet.api.dag.tap.SinkTap;
import com.hazelcast.jet.api.dag.tap.SourceTap;
import com.hazelcast.jet.api.dag.tap.SinkTapWriteStrategy;
import com.hazelcast.jet.api.processor.ProcessorDescriptor;

public interface Vertex extends DagElement {
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
