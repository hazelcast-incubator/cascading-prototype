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

package com.hazelcast.jet.graphgiant;

import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.jet.DummyProcessor;
import com.hazelcast.jet.api.application.Application;
import com.hazelcast.jet.api.dag.DAG;
import com.hazelcast.jet.api.dag.Vertex;
import com.hazelcast.jet.api.hazelcast.JetHazelcastInstance;
import com.hazelcast.jet.api.processor.ProcessorDescriptor;
import com.hazelcast.jet.api.config.JetApplicationConfig;
import com.hazelcast.jet.impl.dag.DAGImpl;
import com.hazelcast.jet.impl.dag.EdgeImpl;
import com.hazelcast.jet.impl.dag.VertexImpl;
import com.hazelcast.jet.impl.hazelcast.JetHazelcast;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class GiantGraphTest {
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

        int branchCount = 5;
        int vertexCount = 500;

        List<IMap<Integer, String>> sinks = new ArrayList<IMap<Integer, String>>(branchCount);

        for (int i = 1; i <= branchCount; i++) {
            sinks.add(instance.<Integer, String>getMap("sinkMap" + i));
        }

        try {
            long t = System.currentTimeMillis();
            JetApplicationConfig confg = new JetApplicationConfig("testApplication");
            instance.getConfig().addCustomServiceConfig(confg);
            confg.setApplicationSecondsToAwait(100000);
            confg.setJetSecondsToAwait(100000);
            confg.setChunkSize(100);
            confg.setContainerQueueSize(128);
            confg.setMaxProcessingThreads(Runtime.getRuntime().availableProcessors());

            Application application = instance.getJetApplication("testApplication");

            DAG dag = new DAGImpl("testApplicationDag");

            Vertex root = new VertexImpl(
                    "root",
                    ProcessorDescriptor.
                            builder(DummyProcessor.Factory.class).
                            withTaskCount(Runtime.getRuntime().availableProcessors()).
                            build()
            );
            dag.addVertex(root);

            root.addSourceMap("sourceMap");

            for (int b = 1; b <= branchCount; b++) {
                Vertex last = root;
                for (int i = 1; i <= vertexCount; i++) {
                    Vertex vertex = new VertexImpl(
                            "v_" + b + "_" + i,
                            ProcessorDescriptor.
                                    builder(DummyProcessor.Factory.class).
                                    withTaskCount(Runtime.getRuntime().availableProcessors()).
                                    build()
                    );

                    dag.addVertex(vertex);
                    dag.addEdge(new EdgeImpl("e_" + b + "_" + i, last, vertex));

                    last = vertex;

                    if (i == vertexCount) {
                        vertex.addSinkMap("sinkMap" + b);
                    }
                }
            }

            System.out.println("Dag created");

            application.submit(dag);

            System.out.println("Dag submitted");

            application.execute().get();

            System.out.println("TotalTime=" + (System.currentTimeMillis() - t));

            for (int i = 1; i <= branchCount; i++) {
                System.out.println(sinks.get(i - 1).size());
                assertEquals(sinks.get(i - 1).size(), CNT);
            }
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
