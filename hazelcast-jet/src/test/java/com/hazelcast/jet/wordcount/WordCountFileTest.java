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

package com.hazelcast.jet.wordcount;


import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.jet.api.container.ContainerDescriptor;
import com.hazelcast.jet.api.strategy.HashingStrategy;
import com.hazelcast.jet.api.strategy.ProcessingStrategy;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.experimental.categories.Category;

import org.Util;
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
import com.hazelcast.jet.api.config.JetApplicationConfig;
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

            Util.warmUpPartitions(instance);

            JetApplicationConfig config = new JetApplicationConfig("testApplication");
            instance.getConfig().addCustomServiceConfig(config);
            config.setApplicationSecondsToAwait(100000);
            config.setJetSecondsToAwait(100000);
            config.setChunkSize(4096);
            config.setContainerQueueSize(65536 * 8);
            config.setMaxProcessingThreads(Runtime.getRuntime().availableProcessors());

            Application application = instance.getJetApplication("testApplication");

            Vertex vertex1 = new VertexImpl(
                    "wordGenerator",
                    ProcessorDescriptor.
                            builder(WordGeneratorProcessor.Factory.class).
                            withTaskCount(Runtime.getRuntime().availableProcessors() / 2).
                            build()
            );

            Vertex vertex2 = new VertexImpl(
                    "wordCounter",
                    ProcessorDescriptor.
                            builder(WordCounterProcessor.Factory.class).
                            withTaskCount(Runtime.getRuntime().availableProcessors()).
                            build()
            );

            for (int i = 1; i <= 1; i++) {
                WordCounterProcessor.time = System.currentTimeMillis();
                vertex1.addSourceFile("/hazelcast_work/partitions/file_1");
            }

            vertex2.addSinkFile("/hazelcast_work/result");

            DAG dag = new DAGImpl("testApplicationDag");

            dag.addVertex(vertex1);
            dag.addVertex(vertex2);
            dag.addEdge(
                    new EdgeImpl.EdgeBuilder(
                            "edge",
                            vertex1,
                            vertex2
                    )
                            .processingStrategy(ProcessingStrategy.PARTITIONING)
                            .hashingStrategy(new HashingStrategy<String, String>() {
                                                 @Override
                                                 public int hash(String object,
                                                                 String partitionKey,
                                                                 ContainerDescriptor containerDescriptor) {
                                                     return partitionKey.hashCode();
                                                 }
                                             }
                            )
                            .partitioningStrategy(new PartitioningStrategy<String>() {
                                @Override
                                public String getPartitionKey(String key) {
                                    return key;
                                }
                            })
                            .build()
            );

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
