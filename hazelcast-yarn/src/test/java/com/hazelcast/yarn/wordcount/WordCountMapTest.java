package com.hazelcast.yarn.wordcount;


import java.io.*;
import java.util.List;
import java.util.ArrayList;

import com.hazelcast.core.IMap;
import com.hazelcast.config.Config;

import java.util.concurrent.Future;

import com.hazelcast.yarn.api.dag.DAG;
import com.hazelcast.yarn.api.dag.Vertex;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.yarn.impl.dag.DAGImpl;
import com.hazelcast.yarn.impl.dag.EdgeImpl;
import com.hazelcast.yarn.impl.dag.VertexImpl;
import com.hazelcast.test.annotation.QuickTest;

import java.util.concurrent.atomic.AtomicInteger;

import com.hazelcast.yarn.api.application.Application;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.yarn.impl.hazelcast.YarnHazelcast;
import com.hazelcast.yarn.api.processor.ProcessorDescriptor;
import com.hazelcast.yarn.api.hazelcast.YarnHazelcastInstance;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;


@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class WordCountMapTest {
    private void fillMapWithData(YarnHazelcastInstance hazelcastInstance)
            throws Exception {
        IMap<String, String> map = hazelcastInstance.getMap("wordtest");

        for (int i = 1; i <= 1; i++) {
            WordCounterProcessor.time = System.currentTimeMillis();
            String file = "/hazelcast_work/partitions/file_" + i;
            LineNumberReader reader = new LineNumberReader(new FileReader(file));
            String line;
            List<Future> f = new ArrayList<Future>();
            StringBuilder sb = new StringBuilder();

            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }

            f.add(map.putAsync(file, sb.toString()));

            for (Future ff : f) {
                ff.get();
            }

            reader.close();
        }
    }

    YarnHazelcastInstance instance;

    @Test
    public void test() throws Exception {
        Config config = new Config();
        config.getMapConfig("wordtest").setInMemoryFormat(InMemoryFormat.OBJECT);
        config.getMapConfig("wordresult").setInMemoryFormat(InMemoryFormat.OBJECT);

        instance = YarnHazelcast.newHazelcastInstance(config);
        try {
            instance.getConfig().getYarnApplicationConfig("testApplication").setApplicationSecondsToAwait(100000);
            instance.getConfig().getYarnApplicationConfig("testApplication").setYarnSecondsToAwait(100000);
            instance.getConfig().getYarnApplicationConfig("testApplication").setTupleChunkSize(1000);
            instance.getConfig().getYarnApplicationConfig("testApplication").setContainerQueueSize(65536);
            instance.getConfig().getYarnApplicationConfig("testApplication").setMaxProcessingThreads(Runtime.getRuntime().availableProcessors());

            this.fillMapWithData(instance);

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

            vertex1.addSourceMap("wordtest");
            vertex2.addSinkMap("wordresult");

            DAG dag = new DAGImpl("testApplicationDag");

            dag.addVertex(vertex1);
            dag.addVertex(vertex2);
            dag.addEdge(new EdgeImpl("edge", vertex1, vertex2));
            application.submit(dag);

            System.out.println("Started");

            for (int i = 0; i < 10; i++) {
                long t = System.currentTimeMillis();
                application.execute().get();
                System.gc();
                System.out.println("TotalTime=" + (System.currentTimeMillis() - t));
            }

            application.finalizeApplication();

            IMap<String, AtomicInteger> result = instance.getMap("wordresult");

            System.out.println("MapSize=" + result.size());

            assertEquals(1024 * 1024 * 10, result.size());
        } catch (Exception e) {
            e.printStackTrace(System.out);
        } finally {
            instance.shutdown();
        }
    }
}
