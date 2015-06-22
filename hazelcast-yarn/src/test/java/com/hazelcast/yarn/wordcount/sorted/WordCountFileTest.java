package com.hazelcast.yarn.wordcount.sorted;

import com.hazelcast.yarn.wordcount.WordGeneratorProcessor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.experimental.categories.Category;

import com.hazelcast.config.Config;
import com.hazelcast.yarn.api.dag.DAG;
import com.hazelcast.yarn.api.dag.Vertex;
import com.hazelcast.yarn.impl.dag.DAGImpl;
import com.hazelcast.yarn.impl.dag.EdgeImpl;
import com.hazelcast.yarn.impl.dag.VertexImpl;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.yarn.api.application.Application;
import com.hazelcast.yarn.impl.hazelcast.YarnHazelcast;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.yarn.api.processor.ProcessorDescriptor;
import com.hazelcast.yarn.api.hazelcast.YarnHazelcastInstance;


@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class WordCountFileTest {
    @Test
    public void test() throws Exception {
        YarnHazelcastInstance instance = YarnHazelcast.newHazelcastInstance(new Config());

        try {
            System.out.println("Started ");

            instance.getConfig().getYarnApplicationConfig("testApplication").setApplicationSecondsToAwait(100000);
            instance.getConfig().getYarnApplicationConfig("testApplication").setYarnSecondsToAwait(100000);
            instance.getConfig().getYarnApplicationConfig("testApplication").setTupleChunkSize(4096);
            instance.getConfig().getYarnApplicationConfig("testApplication").setContainerQueueSize(65536);
            instance.getConfig().getYarnApplicationConfig("testApplication").setMaxProcessingThreads(Runtime.getRuntime().availableProcessors());

            Application application = instance.getYarnApplication("testApplication");

            Vertex vertex1 = new VertexImpl(
                    "wordGenerator",
                    ProcessorDescriptor.
                            builder(WordGeneratorProcessor.Factory.class).
                            withTaskCount(Runtime.getRuntime().availableProcessors()).
                            build()
            );

            Vertex vertex2 = new VertexImpl(
                    "wordCounter",
                    ProcessorDescriptor.
                            builder(WordCounterProcessor.Factory.class).
                            withTaskCount(Runtime.getRuntime().availableProcessors()).
                            build()
            );

            Vertex vertex3 = new VertexImpl(
                    "wordSorter",
                    ProcessorDescriptor.
                            builder(WordSorterProcessor.Factory.class).
                            withTaskCount(1).
                            build()
            );

            for (int i = 1; i <= 271; i++) {
                vertex1.addSourceFile("/hazelcast_work/partitions/file_" + i);
            }

            vertex3.addSinkFile("/hazelcast_work/result");

            DAG dag = new DAGImpl("testApplicationDag");

            dag.addVertex(vertex1);
            dag.addVertex(vertex2);
            dag.addVertex(vertex3);
            dag.addEdge(new EdgeImpl("edge1", vertex1, vertex2));
            dag.addEdge(new EdgeImpl("edge2", vertex2, vertex3));
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
