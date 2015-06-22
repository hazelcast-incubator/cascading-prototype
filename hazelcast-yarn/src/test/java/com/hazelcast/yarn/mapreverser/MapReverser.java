package com.hazelcast.yarn.mapreverser;


import com.hazelcast.core.IMap;
import com.hazelcast.config.Config;
import com.hazelcast.yarn.api.dag.DAG;
import com.hazelcast.yarn.impl.dag.DAGImpl;
import com.hazelcast.yarn.impl.dag.VertexImpl;


import com.hazelcast.config.NetworkConfig;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.yarn.api.application.Application;

import com.hazelcast.yarn.api.dag.Vertex;
import com.hazelcast.yarn.api.hazelcast.YarnHazelcastInstance;
import com.hazelcast.yarn.api.processor.ProcessorDescriptor;
import com.hazelcast.yarn.impl.actor.shuffling.io.ShufflingReceiver;
import com.hazelcast.yarn.impl.actor.shuffling.io.ShufflingSender;
import com.hazelcast.yarn.impl.container.task.processors.shuffling.ShuffledActorTaskProcessor;
import com.hazelcast.yarn.impl.container.task.processors.shuffling.ShuffledConsumerTaskProcessor;
import com.hazelcast.yarn.impl.hazelcast.YarnHazelcast;
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
    public static YarnHazelcastInstance[] hazelCastInstances;

    private static YarnHazelcastInstance buildCluster(int memberCount, Config config) {
        NetworkConfig networkConfig = config.getNetworkConfig();
        networkConfig.getJoin().getMulticastConfig().setEnabled(false);
        networkConfig.getJoin().getTcpIpConfig().setEnabled(true);
        hazelCastInstances = new YarnHazelcastInstance[memberCount];

        List<String> list = new ArrayList<String>();

        for (int i = 0; i < memberCount; i++) {
            list.add("127.0.0.1:" + (5701 + i));
        }

        networkConfig.getJoin().getTcpIpConfig().setMembers(
                list
        );

        for (int i = 0; i < memberCount; i++) {
            hazelCastInstances[i] = YarnHazelcast.newHazelcastInstance(config);
        }

        return hazelCastInstances[0];
    }

    @Test
    public void test() throws ExecutionException, InterruptedException {
        YarnHazelcastInstance instance = buildCluster(5, new Config());
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
            instance.getConfig().getYarnApplicationConfig("testApplication").setApplicationSecondsToAwait(100000);
            instance.getConfig().getYarnApplicationConfig("testApplication").setYarnSecondsToAwait(100000);
            instance.getConfig().getYarnApplicationConfig("testApplication").setTupleChunkSize(4000);
            instance.getConfig().getYarnApplicationConfig("testApplication").setMaxProcessingThreads(Runtime.getRuntime().availableProcessors());

            Application application = instance.getYarnApplication("testApplication");

            Vertex vertex = new VertexImpl(
                    "reverser",
                    ProcessorDescriptor.
                            builder(Reverser.Factory.class).
                            withTaskCount(Runtime.getRuntime().availableProcessors()).
                            build()
            );

            vertex.addSourceMap("source");
            vertex.addSinkMap("target");

            DAG dag = new DAGImpl("testApplicationDag");

            dag.addVertex(vertex);
            application.submit(dag);

            for (int attempt = 0; attempt < 10; attempt++) {
                long t = System.currentTimeMillis();

                application.execute().get();

                System.out.println(
                        " TotalTime=" + (System.currentTimeMillis() - t) +
                                " size=" + targetMap.size() +
                                " ShuffledConsumerTaskProcessor.sctReceiver=" + ShuffledConsumerTaskProcessor.sctReceiver.get() +
                                " ShuffledConsumerTaskProcessor.sctNonReceiver=" + ShuffledConsumerTaskProcessor.sctNonReceiver.get() +
                                " ShuffledConsumerTaskProcessor.sctReceiverByChunk=" + ShuffledConsumerTaskProcessor.sctReceiverByChunk.get() +
                                " ShuffledActorTaskProcessor.fff=" + ShuffledActorTaskProcessor.fff.get() +
                                " Reverser.iii=" + Reverser.ii.get() +
                                " SR=" + ShufflingReceiver.receivedT.get() +
                                " SS=" + ShufflingSender.sendT.get()
                );

                assertEquals(targetMap.size(), CNT);
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
            for (YarnHazelcastInstance inst : hazelCastInstances) {
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
