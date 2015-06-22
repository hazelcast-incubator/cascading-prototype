package com.hazelcast.yarn.wordcount;

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
            instance.getConfig().getYarnApplicationConfig("testApplication").setTupleChunkSize(1000);
            instance.getConfig().getYarnApplicationConfig("testApplication").setContainerQueueSize(65536);
            instance.getConfig().getYarnApplicationConfig("testApplication").setMaxProcessingThreads(2);

            Application application = instance.getYarnApplication("testApplication");

            Vertex vertex1 = new VertexImpl(
                    "wordGenerator",
                    ProcessorDescriptor.
                            builder(WordGeneratorProcessor.Factory.class).
                            withTaskCount(1).
                            build()
            );

            Vertex vertex2 = new VertexImpl(
                    "wordCounter",
                    ProcessorDescriptor.
                            builder(WordCounterProcessor.Factory.class).
                            withTaskCount(1).
                            build()
            );

            for (int i = 1; i <= 1; i++) {
                WordCounterProcessor.time = System.currentTimeMillis();
                vertex1.addSourceFile("/hazelcast_work/partitions/file_1");
            }

            vertex2.addSinkFile("/hazelcast_work/result");

            DAG dag = new DAGImpl("testApplicationDag");

            dag.addVertex(vertex1);
            dag.addVertex(vertex2);
            dag.addEdge(new EdgeImpl("edge", vertex1, vertex2));
            application.submit(dag);

            for (int attempt = 0; attempt < 1; attempt++) {
                application.execute().get();
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
