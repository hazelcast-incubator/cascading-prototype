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

package com.hazelcast.jet.shufflingtest;


import org.junit.Test;

import java.util.List;
import java.util.ArrayList;

import com.hazelcast.core.IMap;
import com.hazelcast.core.IList;
import com.hazelcast.config.Config;

import java.util.concurrent.Future;

import com.hazelcast.jet.api.dag.DAG;
import com.hazelcast.jet.api.dag.Edge;
import com.hazelcast.jet.api.dag.Vertex;
import com.hazelcast.jet.DummyProcessor;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.jet.impl.dag.DAGImpl;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.impl.dag.EdgeImpl;
import com.hazelcast.jet.impl.dag.VertexImpl;

import java.util.concurrent.ExecutionException;

import com.hazelcast.jet.api.application.Application;
import com.hazelcast.jet.impl.hazelcast.JetHazelcast;
import com.hazelcast.jet.api.config.JetApplicationConfig;
import com.hazelcast.jet.api.processor.ProcessorDescriptor;
import com.hazelcast.jet.api.hazelcast.JetHazelcastInstance;
import com.hazelcast.jet.impl.shuffling.IListBasedShufflingStrategy;


public class ShufflingTest {
    private static void fillMapWithData(HazelcastInstance hazelcastInstance)
            throws Exception {
        IMap<Integer, String> map = hazelcastInstance.getMap("source");
        List<Future> futures = new ArrayList<Future>();

        for (int i = 0; i < 50; i++) {
            futures.add(map.putAsync(i, String.valueOf(i)));
        }

        for (Future f : futures) {
            f.get();
        }
    }

    @Test
    public void test() throws Exception {
        JetHazelcastInstance instance = buildCluster(2, new Config());
        JetApplicationConfig config = new JetApplicationConfig("testApplication");

        config.setApplicationSecondsToAwait(100000);
        config.setJetSecondsToAwait(100000);
        config.setChunkSize(10000);
        config.setContainerQueueSize(65536);
        config.setMaxProcessingThreads(Runtime.getRuntime().availableProcessors());

        instance.getConfig().getMapConfig("source").setInMemoryFormat(InMemoryFormat.OBJECT);
        instance.getConfig().addCustomServiceConfig(config);

        long tt = System.currentTimeMillis();
        fillMapWithData(instance);
        System.out.println("Starting... = " + (System.currentTimeMillis() - tt));

        IList<String> targetList = instance.getList("target");

        try {
            tt = System.currentTimeMillis();
            Application application = instance.getJetApplication("testApplication");

            Vertex vertex1 = new VertexImpl(
                    "MapReader",
                    ProcessorDescriptor.
                            builder(DummyProcessor.Factory.class).
                            withTaskCount(1).
                            build()
            );

            Vertex vertex2 = new VertexImpl(
                    "Sorter",
                    ProcessorDescriptor.
                            builder(Lister.Factory.class).
                            withTaskCount(1).
                            build()
            );

            vertex1.addSourceMap("source");
            vertex2.addSinkList("target");

            Edge edge = new EdgeImpl.EdgeBuilder("edge", vertex1, vertex2).
                    shuffling(true).
                    shufflingStrategy(new IListBasedShufflingStrategy("target")).
                    build();

            DAG dag = new DAGImpl("testApplicationDag");

            dag.addVertex(vertex1);
            dag.addVertex(vertex2);
            dag.addEdge(edge);

            application.addResource(DummyProcessor.class);

            application.submit(dag);

            application.execute().get();

            System.out.println("Size=" + targetList.size());

            System.out.println("TotalTime... = " + (System.currentTimeMillis() - tt));

            application.finalizeApplication();
        } catch (Throwable e) {
            if (e instanceof ExecutionException) {
                e.getCause().printStackTrace(System.out);
            } else {
                e.printStackTrace(System.out);
            }
        } finally {
            instance.shutdown();
        }
    }

    private static JetHazelcastInstance buildCluster(int memberCount, Config config) {
        NetworkConfig networkConfig = config.getNetworkConfig();
        networkConfig.getJoin().getMulticastConfig().setEnabled(false);
        networkConfig.getJoin().getTcpIpConfig().setEnabled(true);
        JetHazelcastInstance[] hazelcastInstances = new JetHazelcastInstance[memberCount];

        List<String> list = new ArrayList<String>();

        for (int i = 0; i < memberCount; i++) {
            list.add("127.0.0.1:" + (5701 + i));
        }

        networkConfig.getJoin().getTcpIpConfig().setMembers(
                list
        );

        for (int i = 0; i < memberCount; i++) {
            hazelcastInstances[i] = JetHazelcast.newHazelcastInstance(config);
        }

        return hazelcastInstances[0];
    }
}
