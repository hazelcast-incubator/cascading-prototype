package com.hazelcast.yarn.interruptiontest;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.experimental.categories.Category;

import java.util.List;
import java.util.ArrayList;

import com.hazelcast.core.IMap;

import java.util.concurrent.Future;

import com.hazelcast.yarn.api.dag.DAG;
import com.hazelcast.yarn.api.dag.Vertex;
import com.hazelcast.yarn.impl.dag.DAGImpl;
import com.hazelcast.yarn.impl.dag.EdgeImpl;
import com.hazelcast.yarn.impl.dag.VertexImpl;
import com.hazelcast.test.annotation.QuickTest;

import java.util.concurrent.ExecutionException;


import com.hazelcast.yarn.api.application.Application;
import com.hazelcast.yarn.impl.hazelcast.YarnHazelcast;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.yarn.impl.actor.ringbuffer.RingBuffer;
import com.hazelcast.yarn.api.processor.ProcessorDescriptor;
import com.hazelcast.yarn.api.hazelcast.YarnHazelcastInstance;

import static junit.framework.TestCase.assertFalse;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class InterruptionTest {
    @Test
    public void test() throws ExecutionException, InterruptedException {
        YarnHazelcastInstance instance = YarnHazelcast.newHazelcastInstance();
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
            instance.getConfig().getYarnApplicationConfig("testApplication").setApplicationSecondsToAwait(100000);
            instance.getConfig().getYarnApplicationConfig("testApplication").setYarnSecondsToAwait(100000);
            instance.getConfig().getYarnApplicationConfig("testApplication").setTupleChunkSize(4000);
            instance.getConfig().getYarnApplicationConfig("testApplication").setMaxProcessingThreads(Runtime.getRuntime().availableProcessors());
            final Application application = instance.getYarnApplication("testApplication");

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
                            + " rbPut=" + RingBuffer.RBCounterPut.get()
                            + " rbFetch=" + RingBuffer.RBCounterFetch.get()
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
