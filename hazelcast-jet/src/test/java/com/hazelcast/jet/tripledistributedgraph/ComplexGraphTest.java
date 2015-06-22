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

package com.hazelcast.jet.tripledistributedgraph;


import com.hazelcast.jet.api.config.JetApplicationConfig;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.experimental.categories.Category;

import java.util.List;
import java.util.ArrayList;

import com.hazelcast.core.IMap;

import java.util.concurrent.Future;

import com.hazelcast.jet.api.dag.DAG;
import com.hazelcast.jet.api.dag.Vertex;
import com.hazelcast.jet.DummyProcessor;
import com.hazelcast.jet.impl.dag.DAGImpl;
import com.hazelcast.jet.impl.dag.EdgeImpl;
import com.hazelcast.jet.impl.dag.VertexImpl;

import java.util.concurrent.ExecutionException;

import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.jet.api.application.Application;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.jet.impl.hazelcast.JetHazelcast;
import com.hazelcast.jet.api.processor.ProcessorDescriptor;
import com.hazelcast.jet.api.hazelcast.JetHazelcastInstance;

import static org.junit.Assert.assertEquals;


@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
    public class ComplexGraphTest {
    private static final int CNT = 1000000;

    private void fillSource(String source, JetHazelcastInstance instance) throws ExecutionException, InterruptedException {
        final IMap<Integer, String> sourceMap = instance.getMap(source);

        List<Future> l = new ArrayList<Future>();

        for (int i = 1; i <= CNT; i++) {
            l.add(sourceMap.putAsync(i, String.valueOf(i)));
        }

        for (Future f : l) {
            f.get();
        }
    }

    @Test
    public void test() throws ExecutionException, InterruptedException {
        JetHazelcastInstance instance = JetHazelcast.newHazelcastInstance();
        fillSource("sourceMap", instance);

        final IMap<Integer, String> sinkMap1 = instance.getMap("sinkMap1");
        final IMap<Integer, String> sinkMap2 = instance.getMap("sinkMap2");

        try {
            long t = System.currentTimeMillis();
            JetApplicationConfig config = new JetApplicationConfig("testApplication");

            config.setApplicationSecondsToAwait(100000);
            config.setJetSecondsToAwait(100000);
            config.setChunkSize(4000);
            config.setMaxProcessingThreads(Runtime.getRuntime().availableProcessors());

            instance.getConfig().addCustomServiceConfig(config);
            Application application = instance.getJetApplication("testApplication");

            Vertex root = new VertexImpl(
                    "root",
                    ProcessorDescriptor.
                            builder(DummyProcessor.Factory.class).
                            withTaskCount(Runtime.getRuntime().availableProcessors()).
                            build()
            );

            Vertex vertex11 = new VertexImpl(
                    "v11",
                    ProcessorDescriptor.
                            builder(DummyProcessor.Factory.class).
                            withTaskCount(Runtime.getRuntime().availableProcessors()).
                            build()
            );

            Vertex vertex12 = new VertexImpl(
                    "v12",
                    ProcessorDescriptor.
                            builder(DummyProcessor.Factory.class).
                            withTaskCount(Runtime.getRuntime().availableProcessors()).
                            build()
            );

            Vertex vertex21 = new VertexImpl(
                    "v21",
                    ProcessorDescriptor.
                            builder(DummyProcessor.Factory.class).
                            withTaskCount(Runtime.getRuntime().availableProcessors()).
                            build()
            );

            Vertex vertex22 = new VertexImpl(
                    "v22",
                    ProcessorDescriptor.
                            builder(DummyProcessor.Factory.class).
                            withTaskCount(Runtime.getRuntime().availableProcessors()).
                            build()
            );

            root.addSourceMap("sourceMap");

            vertex12.addSinkMap("sinkMap1");
            vertex22.addSinkMap("sinkMap2");

            DAG dag = new DAGImpl("testApplicationDag");

            dag.addVertex(root);
            dag.addVertex(vertex11);
            dag.addVertex(vertex12);
            dag.addVertex(vertex21);
            dag.addVertex(vertex22);

            dag.addEdge(new EdgeImpl("edge1", root, vertex11));
            dag.addEdge(new EdgeImpl("edge2", root, vertex21));
            dag.addEdge(new EdgeImpl("edge3", vertex11, vertex12));
            dag.addEdge(new EdgeImpl("edge4", vertex21, vertex22));

            application.submit(dag);
            application.execute().get();

            System.out.println("TotalTime=" +
                            (System.currentTimeMillis() - t) +
                            " sinkMap1.size=" + sinkMap1.size() +
                            " sinkMap2.size=" + sinkMap2.size()
            );

            assertEquals(sinkMap1.size(), CNT);
            assertEquals(sinkMap2.size(), CNT);
        } catch (Exception e) {
            if (e.getCause() != null) {
                Throwable err = e;

                while (err.getCause() != null) {
                    err = err.getCause();
                }

                err.printStackTrace(System.out);
            } else {
                e.printStackTrace(System.out);
            }
        } finally {
            instance.shutdown();
        }
    }
}
