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

package com.hazelcast.jet.simpletest;


import java.util.List;
import java.util.ArrayList;

import com.hazelcast.core.IMap;

import java.util.concurrent.Future;

import com.hazelcast.config.Config;
import com.hazelcast.jet.api.dag.DAG;
import com.hazelcast.jet.api.dag.Vertex;
import com.hazelcast.jet.impl.dag.DAGImpl;
import com.hazelcast.jet.impl.dag.EdgeImpl;
import com.hazelcast.jet.impl.dag.VertexImpl;
import com.hazelcast.test.annotation.QuickTest;

import java.util.concurrent.ExecutionException;

import com.hazelcast.jet.TestJetHazelcastFactory;
import com.hazelcast.jet.api.application.Application;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.jet.api.config.JetApplicationConfig;
import com.hazelcast.jet.api.processor.ProcessorDescriptor;
import com.hazelcast.jet.api.hazelcast.JetHazelcastInstance;
import com.hazelcast.jet.impl.hazelcast.client.JetHazelcastClientProxy;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class})
public class SimpleTest {
    private final TestJetHazelcastFactory hazelcastFactory = new TestJetHazelcastFactory();

    private JetHazelcastInstance server;
    private JetHazelcastClientProxy client;

    @Before
    public void setup() {
        Config config = new Config();
        JetApplicationConfig jetConfig = new JetApplicationConfig("testApplication");
        jetConfig.setApplicationSecondsToAwait(100000);
        jetConfig.setJetSecondsToAwait(100000);
        jetConfig.setChunkSize(4000);
        jetConfig.setMaxProcessingThreads(
                Runtime.getRuntime().availableProcessors());
        config.addCustomServiceConfig(jetConfig);

        this.server = this.hazelcastFactory.newHazelcastInstance(config);
        this.client = (JetHazelcastClientProxy) this.hazelcastFactory.newHazelcastClient(null);
        this.client.getClientConfig().addCustomServiceConfig(jetConfig);
    }

    @Test
    public void serverTest() throws ExecutionException, InterruptedException {
        run(
                this.server
        );
    }

    @Test
    public void clientTest() throws ExecutionException, InterruptedException {
        run(
                this.client
        );
    }

    private void run(JetHazelcastInstance instance) throws ExecutionException, InterruptedException {
        final IMap<Integer, String> sourceMap = instance.getMap("source");
        final IMap<Integer, String> targetMap = instance.getMap("target");
        int CNT = 1;

        List<Future> l = new ArrayList<Future>();

        for (int i = 1; i <= CNT; i++) {
            l.add(sourceMap.putAsync(i, String.valueOf(i)));
        }

        System.out.println("Waiting ....");

        for (Future f : l) {
            f.get();
        }

        System.out.println("Running ....");

        try {
            Application application = instance.getJetApplication("testApplication");

            Vertex vertex1 = new VertexImpl(
                    "mod1",
                    ProcessorDescriptor.
                            builder(FilterMod2.Factory.class).
                            withTaskCount(Runtime.getRuntime().availableProcessors()).
                            build()
            );

            Vertex vertex2 = new VertexImpl(
                    "mod2",
                    ProcessorDescriptor.
                            builder(FilterMod2.Factory.class).
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

            for (int i = 0; i < 1; i++) {
                long t = System.currentTimeMillis();

                application.execute().get();

                System.out.println("TotalTime=" + (System.currentTimeMillis() - t)
                                + " size=" + targetMap.size()
                );

                assertEquals(targetMap.size(), CNT);
                targetMap.clear();
            }

            application.finalizeApplication();
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

    @After
    public void tearDown() {
        this.hazelcastFactory.terminateAll();
    }
}
