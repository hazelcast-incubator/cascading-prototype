package com.hazelcast.yarn.tripledistributedgraph;


import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.experimental.categories.Category;

import java.util.List;
import java.util.ArrayList;

import com.hazelcast.core.IMap;

import java.util.concurrent.Future;

import com.hazelcast.yarn.api.dag.DAG;
import com.hazelcast.yarn.api.dag.Vertex;
import com.hazelcast.yarn.DummyProcessor;
import com.hazelcast.yarn.impl.dag.DAGImpl;
import com.hazelcast.yarn.impl.dag.EdgeImpl;
import com.hazelcast.yarn.impl.dag.VertexImpl;

import java.util.concurrent.ExecutionException;

import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.yarn.api.application.Application;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.yarn.impl.hazelcast.YarnHazelcast;
import com.hazelcast.yarn.api.processor.ProcessorDescriptor;
import com.hazelcast.yarn.api.hazelcast.YarnHazelcastInstance;

import static org.junit.Assert.assertEquals;


@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class ComplexGraphTest {
    private static final int CNT = 1000000;

    private void fillSource(String source, YarnHazelcastInstance instance) throws ExecutionException, InterruptedException {
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
        YarnHazelcastInstance instance = YarnHazelcast.newHazelcastInstance();
        fillSource("sourceMap", instance);

        final IMap<Integer, String> sinkMap1 = instance.getMap("sinkMap1");
        final IMap<Integer, String> sinkMap2 = instance.getMap("sinkMap2");

        try {
            long t = System.currentTimeMillis();
            instance.getConfig().getYarnApplicationConfig("testApplication").setApplicationSecondsToAwait(100000);
            instance.getConfig().getYarnApplicationConfig("testApplication").setYarnSecondsToAwait(100000);
            instance.getConfig().getYarnApplicationConfig("testApplication").setTupleChunkSize(4000);
            instance.getConfig().getYarnApplicationConfig("testApplication").setMaxProcessingThreads(Runtime.getRuntime().availableProcessors());
            Application application = instance.getYarnApplication("testApplication");

            Vertex root = new VertexImpl(
                    "root",
                    ProcessorDescriptor.
                            builder(DummyProcessor.Factory.class).
                            withTaskCount(Runtime.getRuntime().availableProcessors()).
                            build()
            );

            Vertex vertex11 = new VertexImpl(
                    "v11",
                    ProcessorDescriptor.
                            builder(DummyProcessor.Factory.class).
                            withTaskCount(Runtime.getRuntime().availableProcessors()).
                            build()
            );

            Vertex vertex12 = new VertexImpl(
                    "v12",
                    ProcessorDescriptor.
                            builder(DummyProcessor.Factory.class).
                            withTaskCount(Runtime.getRuntime().availableProcessors()).
                            build()
            );

            Vertex vertex21 = new VertexImpl(
                    "v21",
                    ProcessorDescriptor.
                            builder(DummyProcessor.Factory.class).
                            withTaskCount(Runtime.getRuntime().availableProcessors()).
                            build()
            );

            Vertex vertex22 = new VertexImpl(
                    "v22",
                    ProcessorDescriptor.
                            builder(DummyProcessor.Factory.class).
                            withTaskCount(Runtime.getRuntime().availableProcessors()).
                            build()
            );

            root.addSourceMap("sourceMap");

            vertex12.addSinkMap("sinkMap1");
            vertex22.addSinkMap("sinkMap2");

            DAG dag = new DAGImpl("testApplicationDag");

            dag.addVertex(root);
            dag.addVertex(vertex11);
            dag.addVertex(vertex12);
            dag.addVertex(vertex21);
            dag.addVertex(vertex22);

            dag.addEdge(new EdgeImpl("edge1", root, vertex11));
            dag.addEdge(new EdgeImpl("edge2", root, vertex21));
            dag.addEdge(new EdgeImpl("edge3", vertex11, vertex12));
            dag.addEdge(new EdgeImpl("edge4", vertex21, vertex22));

            application.submit(dag);
            application.execute().get();

            System.out.println("TotalTime=" +
                            (System.currentTimeMillis() - t) +
                            " sinkMap1.size=" + sinkMap1.size() +
                            " sinkMap2.size=" + sinkMap2.size()
            );

            assertEquals(sinkMap1.size(), CNT);
            assertEquals(sinkMap2.size(), CNT);
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
