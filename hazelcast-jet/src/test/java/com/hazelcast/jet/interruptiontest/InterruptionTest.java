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

package com.hazelcast.jet.interruptiontest;

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
import com.hazelcast.jet.impl.dag.DAGImpl;
import com.hazelcast.jet.impl.dag.EdgeImpl;
import com.hazelcast.jet.impl.dag.VertexImpl;
import com.hazelcast.test.annotation.QuickTest;

import java.util.concurrent.ExecutionException;


import com.hazelcast.jet.api.application.Application;
import com.hazelcast.jet.impl.hazelcast.JetHazelcast;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.jet.api.processor.ProcessorDescriptor;
import com.hazelcast.jet.api.hazelcast.JetHazelcastInstance;

import static junit.framework.TestCase.assertFalse;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class InterruptionTest {
    @Test
    public void test() throws ExecutionException, InterruptedException {
        JetHazelcastInstance instance = JetHazelcast.newHazelcastInstance();
        final IMap<Integer, String> sourceMap = instance.getMap("source");
        final IMap<Integer, String> targetMap = instance.getMap("target");

        int CNT = 1000000;

        List<Future> l = new ArrayList<Future>();

        for (int i = 1; i <= CNT; i++) {
            l.add(sourceMap.putAsync(i, String.valueOf(i)));
        }

        for (Future f : l) {
            f.get();
        }

        try {
            JetApplicationConfig confg = new JetApplicationConfig("testApplication");
            instance.getConfig().addCustomServiceConfig(confg);
            confg.setApplicationSecondsToAwait(100000);
            confg.setJetSecondsToAwait(100000);
            confg.setChunkSize(4000);
            confg.setMaxProcessingThreads(Runtime.getRuntime().availableProcessors());
            final Application application = instance.getJetApplication("testApplication");

            Vertex vertex1 = new VertexImpl(
                    "dummy1",
                    ProcessorDescriptor.
                            builder(InterruptionProcessor.Factory.class).
                            withTaskCount(Runtime.getRuntime().availableProcessors()).
                            build()
            );

            Vertex vertex2 = new VertexImpl(
                    "dummy2",
                    ProcessorDescriptor.
                            builder(CachingProcessor.Factory.class).
                            withTaskCount(Runtime.getRuntime().availableProcessors()).
                            build()
            );

            vertex1.addSourceMap("source");
            vertex2.addSinkMap("target");
            DAG dag = new DAGImpl("testApplicationDag");

            dag.addVertex(vertex1);
            dag.addVertex(vertex2);
            dag.addEdge(new EdgeImpl("edge", vertex1, vertex2));
            application.submit(dag);

            System.out.println("Submitted");

            long t = System.currentTimeMillis();

            Future executionFuture = application.execute();

            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        Thread.sleep(1000);
                        System.out.println("Interrupting");
                        application.interrupt().get();
                        System.out.println("Interrupted");
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
                    }
                }
            }).start();

            try {
                executionFuture.get();
                assertFalse(true);
            } catch (Throwable e) {

            }

            System.out.println("Executing again");

            application.execute().get();

            System.out.println(
                    "TotalTime=" + (System.currentTimeMillis() - t) + " size=" + targetMap.size()
            );

            targetMap.clear();
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
