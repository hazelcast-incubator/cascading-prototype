package com.hazelcast.yarn.graphgiant;

import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.yarn.DummyProcessor;
import com.hazelcast.yarn.api.application.Application;
import com.hazelcast.yarn.api.dag.DAG;
import com.hazelcast.yarn.api.dag.Vertex;
import com.hazelcast.yarn.api.hazelcast.YarnHazelcastInstance;
import com.hazelcast.yarn.api.processor.ProcessorDescriptor;
import com.hazelcast.yarn.impl.dag.DAGImpl;
import com.hazelcast.yarn.impl.dag.EdgeImpl;
import com.hazelcast.yarn.impl.dag.VertexImpl;
import com.hazelcast.yarn.impl.hazelcast.YarnHazelcast;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class GiantGraphTest {
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

        int branchCount = 5;
        int vertexCount = 500;

        List<IMap<Integer, String>> sinks = new ArrayList<IMap<Integer, String>>(branchCount);

        for (int i = 1; i <= branchCount; i++) {
            sinks.add(instance.<Integer, String>getMap("sinkMap" + i));
        }

        try {
            long t = System.currentTimeMillis();
            instance.getConfig().getYarnApplicationConfig("testApplication").setApplicationSecondsToAwait(100000);
            instance.getConfig().getYarnApplicationConfig("testApplication").setYarnSecondsToAwait(100000);
            instance.getConfig().getYarnApplicationConfig("testApplication").setTupleChunkSize(100);
            instance.getConfig().getYarnApplicationConfig("testApplication").setContainerQueueSize(128);
            instance.getConfig().getYarnApplicationConfig("testApplication").setMaxProcessingThreads(Runtime.getRuntime().availableProcessors());
            Application application = instance.getYarnApplication("testApplication");

            DAG dag = new DAGImpl("testApplicationDag");

            Vertex root = new VertexImpl(
                    "root",
                    ProcessorDescriptor.
                            builder(DummyProcessor.Factory.class).
                            withTaskCount(Runtime.getRuntime().availableProcessors()).
                            build()
            );
            dag.addVertex(root);

            root.addSourceMap("sourceMap");

            for (int b = 1; b <= branchCount; b++) {
                Vertex last = root;
                for (int i = 1; i <= vertexCount; i++) {
                    Vertex vertex = new VertexImpl(
                            "v_" + b + "_" + i,
                            ProcessorDescriptor.
                                    builder(DummyProcessor.Factory.class).
                                    withTaskCount(Runtime.getRuntime().availableProcessors()).
                                    build()
                    );

                    dag.addVertex(vertex);
                    dag.addEdge(new EdgeImpl("e_" + b + "_" + i, last, vertex));

                    last = vertex;

                    if (i == vertexCount) {
                        vertex.addSinkMap("sinkMap" + b);
                    }
                }
            }

            System.out.println("Dag created");

            application.submit(dag);

            System.out.println("Dag submitted");

            application.execute().get();

            System.out.println("TotalTime=" + (System.currentTimeMillis() - t));

            for (int i = 1; i <= branchCount; i++) {
                System.out.println(sinks.get(i - 1).size());
                assertEquals(sinks.get(i - 1).size(), CNT);
            }
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
