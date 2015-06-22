package com.hazelcast.yarn.wordcount.offheap;

import org.junit.Test;
import com.hazelcast.config.Config;
import com.hazelcast.yarn.api.dag.DAG;
import com.hazelcast.yarn.api.dag.Vertex;
import com.hazelcast.yarn.impl.dag.DAGImpl;
import com.hazelcast.yarn.impl.dag.EdgeImpl;
import com.hazelcast.yarn.impl.dag.VertexImpl;
import com.hazelcast.yarn.api.application.Application;
import com.hazelcast.yarn.impl.hazelcast.YarnHazelcast;
import com.hazelcast.yarn.api.processor.ProcessorDescriptor;
import com.hazelcast.yarn.api.hazelcast.YarnHazelcastInstance;


public class WordCountOffHeapFileTest {
    @Test
    public void test() throws Exception {
        Config config = new Config();
        YarnHazelcastInstance instance = YarnHazelcast.newHazelcastInstance(config);

        try {
            System.out.println("Started ");

            instance.getConfig().getYarnApplicationConfig("testApplication").setApplicationSecondsToAwait(100000);
            instance.getConfig().getYarnApplicationConfig("testApplication").setYarnSecondsToAwait(100000);
            instance.getConfig().getYarnApplicationConfig("testApplication").setTupleChunkSize(65536);
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

            for (int i = 1; i <= 8; i++) {
                vertex1.addSourceHDFile("/hazelcast_work/partitions/file_partitions" + i);
            }

            vertex2.addSinkHDFile("/hazelcast_work/result");

            DAG dag = new DAGImpl("testApplicationDag");

            dag.addVertex(vertex1);
            dag.addVertex(vertex2);
            dag.addEdge(new EdgeImpl("edge", vertex1, vertex2));
            application.submit(dag);

            for (int attempt = 0; attempt < 1; attempt++) {
                long t = System.currentTimeMillis();
                WordCounterProcessor.time = System.currentTimeMillis();
                application.execute().get();
                System.out.println("TotalTime=" + (System.currentTimeMillis() - t));
            }

            application.finalizeApplication();
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
