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

package com.hazelcast.jet.mapreverser;


import com.hazelcast.core.IMap;
import com.hazelcast.config.Config;
import com.hazelcast.jet.api.config.JetApplicationConfig;
import com.hazelcast.jet.api.dag.DAG;
import com.hazelcast.jet.impl.dag.DAGImpl;
import com.hazelcast.jet.impl.dag.VertexImpl;


import com.hazelcast.config.NetworkConfig;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.jet.api.application.Application;

import com.hazelcast.jet.api.dag.Vertex;
import com.hazelcast.jet.api.hazelcast.JetHazelcastInstance;
import com.hazelcast.jet.api.processor.ProcessorDescriptor;
import com.hazelcast.jet.impl.hazelcast.JetHazelcast;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class MapReverser {
    public static JetHazelcastInstance[] hazelCastInstances;

    private static JetHazelcastInstance buildCluster(int memberCount, Config config) {
        NetworkConfig networkConfig = config.getNetworkConfig();
        networkConfig.getJoin().getMulticastConfig().setEnabled(false);
        networkConfig.getJoin().getTcpIpConfig().setEnabled(true);
        hazelCastInstances = new JetHazelcastInstance[memberCount];

        List<String> list = new ArrayList<String>();

        for (int i = 0; i < memberCount; i++) {
            list.add("127.0.0.1:" + (5701 + i));
        }

        networkConfig.getJoin().getTcpIpConfig().setMembers(
                list
        );

        for (int i = 0; i < memberCount; i++) {
            hazelCastInstances[i] = JetHazelcast.newHazelcastInstance(config);
        }

        return hazelCastInstances[0];
    }

    @Test
    public void test() throws ExecutionException, InterruptedException {
        JetHazelcastInstance instance = buildCluster(5, new Config());
        System.out.println("Members=" + instance.getCluster().getMembers() + " Current=" + instance.getCluster().getLocalMember().getAddress());

        final IMap<Integer, String> sourceMap = instance.getMap("source");
        final IMap<String, Integer> targetMap = instance.getMap("target");

        int CNT = 1000000;

        List<Future> l = new ArrayList<Future>();

        for (int i = 1; i <= CNT; i++) {
            l.add(sourceMap.putAsync(i, "s_" + String.valueOf(CNT - i + 1)));
        }

        for (Future f : l) {
            f.get();
        }

        System.out.println("Started " + sourceMap.size());

        try {
            JetApplicationConfig confg = new JetApplicationConfig("testApplication");
            instance.getConfig().addCustomServiceConfig(confg);

            confg.setApplicationSecondsToAwait(100000);
            confg.setJetSecondsToAwait(100000);
            confg.setChunkSize(4000);
            confg.setMaxProcessingThreads(Runtime.getRuntime().availableProcessors());

            Application application = instance.getJetApplication("testApplication");

            Vertex vertex = new VertexImpl(
                    "reverser",
                    ProcessorDescriptor.
                            builder(Reverser.Factory.class).
                            withTaskCount(1).
                            build()
            );

            vertex.addSourceMap("source");
            vertex.addSinkMap("target");

            DAG dag = new DAGImpl("testApplicationDag");

            dag.addVertex(vertex);
            application.submit(dag);

            for (int attempt = 0; attempt < 1; attempt++) {
                long t = System.currentTimeMillis();

                application.execute().get();

                System.out.println(
                        " TotalTime=" + (System.currentTimeMillis() - t) +
                                " size=" + targetMap.size() +
                                " Reverser.iii=" + Reverser.ii.get()
                );

                assertEquals(CNT, targetMap.size());
                targetMap.clear();
                System.out.println("Executed " + attempt);
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
            for (JetHazelcastInstance inst : hazelCastInstances) {
                try {
                    System.out.println("Shutdown " + inst.getCluster().getLocalMember().getAddress());
                    inst.shutdown();
                } catch (Throwable ee) {
                    ee.printStackTrace(System.out);
                }
            }

            System.out.println("Shutdown complete");
            System.exit(0);
        }
    }
}
