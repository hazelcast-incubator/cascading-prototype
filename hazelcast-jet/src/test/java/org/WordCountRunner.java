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

package org;

import com.hazelcast.jet.api.dag.DAG;
import com.hazelcast.jet.api.dag.Edge;
import com.hazelcast.jet.api.dag.Vertex;
import com.hazelcast.jet.impl.dag.DAGImpl;
import com.hazelcast.jet.impl.dag.EdgeImpl;
import com.hazelcast.jet.impl.dag.VertexImpl;
import com.hazelcast.jet.api.application.Application;
import com.hazelcast.jet.api.processor.ProcessorDescriptor;
import com.hazelcast.jet.api.hazelcast.JetHazelcastInstance;
import com.hazelcast.jet.impl.shuffling.IListBasedShufflingStrategy;

public class WordCountRunner {
    public static void main(String args[]) {
        Integer count = Integer.valueOf(args[1]);
        String file = args[2];
        String target = args[3];

        HzNodeStart.main(args);
        JetHazelcastInstance instance = HzNodeStart.instance;

        try {
            System.out.println("Started " + instance.getCluster().getMembers().size() + " count=" + count);

            Util.warmUpPartitions(instance);

            Application application = instance.getJetApplication("testApplication");

            Vertex vertex1 = new VertexImpl(
                    "wordGenerator",
                    ProcessorDescriptor.
                            builder(WordGeneratorProcessor.Factory.class).
                            withTaskCount(3).
                            build()
            );

            Vertex vertex2 = new VertexImpl(
                    "wordCounter",
                    ProcessorDescriptor.
                            builder(WordCounterProcessor.Factory.class).
                            withTaskCount(3).
                            build()
            );

            Vertex vertex3 = new VertexImpl(
                    "wordCollector",
                    ProcessorDescriptor.
                            builder(WordCountCollectorProcessor.Factory.class).
                            withTaskCount(3).
                            build()
            );

            for (int i = 1; i <= count; i++) {
                vertex1.addSourceFile(file);
            }

            vertex3.addSinkFile(target);

            DAG dag = new DAGImpl("testApplicationDag");

            dag.addVertex(vertex1);

            dag.addVertex(vertex2);

            dag.addVertex(vertex3);

            dag.addEdge(new EdgeImpl("edge1", vertex1, vertex2));

            dag.addEdge(
                    new EdgeImpl.EdgeBuilder("edge2", vertex1, vertex2).
                            shuffling(true).
                            shufflingStrategy(new ShufflingStrategyImpl(instance.getCluster().getLocalMember().getAddress())).
                            build()
            );

            System.out.println("BeforeSubmit");

            application.submit(dag);
            for (int i = 0; i < 1; i++) {
                long t = System.currentTimeMillis();
                System.out.println("Started");
                application.execute().get();
                System.out.println("TotalTime=" + (System.currentTimeMillis() - t));
            }

            application.finalizeApplication().get();
        } catch (Throwable e) {
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
