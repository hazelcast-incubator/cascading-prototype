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

package com.hazelcast.jet.wordcount.sorted;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.experimental.categories.Category;

import com.hazelcast.config.Config;
import com.hazelcast.jet.api.dag.DAG;
import com.hazelcast.jet.api.dag.Vertex;
import com.hazelcast.jet.impl.dag.DAGImpl;
import com.hazelcast.jet.impl.dag.EdgeImpl;
import com.hazelcast.jet.impl.dag.VertexImpl;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.jet.api.application.Application;
import com.hazelcast.jet.impl.hazelcast.JetHazelcast;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.jet.wordcount.WordCounterProcessor;
import com.hazelcast.jet.api.config.JetApplicationConfig;
import com.hazelcast.jet.wordcount.WordGeneratorProcessor;
import com.hazelcast.jet.api.processor.ProcessorDescriptor;
import com.hazelcast.jet.api.hazelcast.JetHazelcastInstance;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class WordCountFileTest {
    @Test
    public void test() throws Exception {
        JetHazelcastInstance instance = JetHazelcast.newHazelcastInstance(new Config());

        try {
            System.out.println("Started ");

            JetApplicationConfig jetApplicationConfig = new JetApplicationConfig("testApplication");

            jetApplicationConfig.setApplicationSecondsToAwait(100000);
            jetApplicationConfig.setJetSecondsToAwait(100000);
            jetApplicationConfig.setChunkSize(4096);
            jetApplicationConfig.setContainerQueueSize(65536);
            jetApplicationConfig.setMaxProcessingThreads(Runtime.getRuntime().availableProcessors());

            instance.getConfig().addCustomServiceConfig(jetApplicationConfig);

            Application application = instance.getJetApplication("testApplication");

            Vertex vertex1 = new VertexImpl(
                    "wordGenerator",
                    ProcessorDescriptor.
                            builder(WordGeneratorProcessor.Factory.class).
                            withTaskCount(Runtime.getRuntime().availableProcessors()).
                            build()
            );

            Vertex vertex2 = new VertexImpl(
                    "wordCounter",
                    ProcessorDescriptor.
                            builder(WordCounterProcessor.Factory.class).
                            withTaskCount(Runtime.getRuntime().availableProcessors()).
                            build()
            );

            Vertex vertex3 = new VertexImpl(
                    "wordSorter",
                    ProcessorDescriptor.
                            builder(WordSorterProcessor.Factory.class).
                            withTaskCount(1).
                            build()
            );

            for (int i = 1; i <= 271; i++) {
                vertex1.addSourceFile("/hazelcast_work/partitions/file_" + i);
            }

            vertex3.addSinkFile("/hazelcast_work/result");

            DAG dag = new DAGImpl("testApplicationDag");

            dag.addVertex(vertex1);
            dag.addVertex(vertex2);
            dag.addVertex(vertex3);
            dag.addEdge(new EdgeImpl("edge1", vertex1, vertex2));
            dag.addEdge(new EdgeImpl("edge2", vertex2, vertex3));
            application.submit(dag);

            for (int attempt = 0; attempt < 1; attempt++) {
                long t = System.currentTimeMillis();
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
