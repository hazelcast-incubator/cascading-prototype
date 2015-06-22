package com.hazelcast.yarn.shufflingtest;

import com.hazelcast.yarn.DummyProcessor;
import com.hazelcast.yarn.impl.actor.ringbuffer.RingBuffer;
import com.hazelcast.yarn.impl.actor.shuffling.io.ShufflingReceiver;
import com.hazelcast.yarn.impl.actor.shuffling.io.ShufflingSender;
import com.hazelcast.yarn.impl.container.task.processors.shuffling.ShuffledConsumerTaskProcessor;
import com.hazelcast.yarn.impl.tap.source.HazelcastMapPartitionReader;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

import com.hazelcast.core.IMap;
import com.hazelcast.core.IList;

import com.hazelcast.config.Config;
import com.hazelcast.yarn.api.dag.DAG;
import com.hazelcast.yarn.api.dag.Edge;
import com.hazelcast.yarn.api.dag.Vertex;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.yarn.impl.dag.DAGImpl;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.yarn.impl.dag.EdgeImpl;
import com.hazelcast.yarn.impl.dag.VertexImpl;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.hazelcast.yarn.api.application.Application;
import com.hazelcast.yarn.impl.hazelcast.YarnHazelcast;
import com.hazelcast.yarn.api.processor.ProcessorDescriptor;
import com.hazelcast.yarn.api.hazelcast.YarnHazelcastInstance;
import com.hazelcast.yarn.impl.shuffling.IListBasedShufflingStrategy;

public class ShufflingTest {
    private static void fillMapWithData(HazelcastInstance hazelcastInstance)
            throws Exception {
        IMap<Integer, String> map = hazelcastInstance.getMap("source");
        List<Future> futures = new ArrayList<Future>();

        for (int i = 0; i < 1000000; i++) {
            futures.add(map.putAsync(i, String.valueOf(i)));
        }

        for (Future f : futures) {
            f.get();
        }
    }

    @Test
    public void test() throws Exception {
        Config config = new Config();

        config.getYarnApplicationConfig("testApplication").setApplicationSecondsToAwait(100000);
        config.getYarnApplicationConfig("testApplication").setYarnSecondsToAwait(100000);
        config.getYarnApplicationConfig("testApplication").setTupleChunkSize(4000);
        config.getYarnApplicationConfig("testApplication").setMaxProcessingThreads(Runtime.getRuntime().availableProcessors());
        config.getMapConfig("source").setInMemoryFormat(InMemoryFormat.OBJECT);
        YarnHazelcastInstance instance = buildCluster(2, config);

        long tt = System.currentTimeMillis();
        fillMapWithData(instance);
        System.out.println("Starting... = " + (System.currentTimeMillis() - tt));

        IList<String> targetList = instance.getList("target");

        try {
            tt = System.currentTimeMillis();
            Application application = instance.getYarnApplication("testApplication");

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

            Edge edge = new EdgeImpl("vertex", vertex1, vertex2, true, new IListBasedShufflingStrategy("target"));

            DAG dag = new DAGImpl("testApplicationDag");

            dag.addVertex(vertex1);
            dag.addVertex(vertex2);
            dag.addEdge(edge);

            application.addResource(DummyProcessor.class);

            application.submit(dag);

            application.execute().get();

            System.out.println("Size=" + targetList.size() + " SR=" + ShufflingReceiver.receivedT.get() +
                            " SS=" + ShufflingSender.sendT.get() +
                            " read1=" + HazelcastMapPartitionReader.read1.get() +
                            " read2=" + HazelcastMapPartitionReader.read2.get() +
                            " RBCounterFetch=" + RingBuffer.RBCounterFetch.get() +
                            " RBCounterPut=" + RingBuffer.RBCounterPut.get()
            );

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

    private static YarnHazelcastInstance buildCluster(int memberCount, Config config) {
        NetworkConfig networkConfig = config.getNetworkConfig();
        networkConfig.getJoin().getMulticastConfig().setEnabled(false);
        networkConfig.getJoin().getTcpIpConfig().setEnabled(true);
        YarnHazelcastInstance[] hazelcastInstances = new YarnHazelcastInstance[memberCount];

        List<String> list = new ArrayList<String>();

        for (int i = 0; i < memberCount; i++) {
            list.add("127.0.0.1:" + (5701 + i));
        }

        networkConfig.getJoin().getTcpIpConfig().setMembers(
                list
        );

        for (int i = 0; i < memberCount; i++) {
            hazelcastInstances[i] = YarnHazelcast.newHazelcastInstance(config);
        }

        return hazelcastInstances[0];
    }
}
