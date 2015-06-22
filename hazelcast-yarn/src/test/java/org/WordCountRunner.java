package org;

import com.hazelcast.yarn.api.dag.DAG;
import com.hazelcast.yarn.api.dag.Vertex;
import com.hazelcast.yarn.impl.dag.DAGImpl;
import com.hazelcast.yarn.impl.dag.EdgeImpl;
import com.hazelcast.yarn.impl.dag.VertexImpl;
import com.hazelcast.yarn.api.application.Application;
import com.hazelcast.yarn.api.processor.ProcessorDescriptor;
import com.hazelcast.yarn.api.hazelcast.YarnHazelcastInstance;

public class WordCountRunner {
    public static void main(String args[]) {
        Integer count = Integer.valueOf(args[1]);
        String file = args[2];
        String target = args[3];

        HzNodeStart.main(args);

        YarnHazelcastInstance instance = HzNodeStart.instance;

        try {
            System.out.println("Started " + instance.getCluster().getMembers().size() + " count=" + count);

            Util.warmUpPartitions(instance);

            Application application = instance.getYarnApplication("testApplication");

            Vertex vertex1 = new VertexImpl(
                    "wordGenerator",
                    ProcessorDescriptor.
                            builder(WordGeneratorProcessor.Factory.class).
                            withTaskCount(Math.min(count, Runtime.getRuntime().availableProcessors())).
                            build()
            );

            Vertex vertex2 = new VertexImpl(
                    "wordCounter",
                    ProcessorDescriptor.
                            builder(WordCounterProcessor.Factory.class).
                            withTaskCount(Math.min(count, Runtime.getRuntime().availableProcessors())).
                            build()
            );

            Vertex vertex3 = new VertexImpl(
                    "wordCollector",
                    ProcessorDescriptor.
                            builder(WordCountCollectorProcessor.Factory.class).
                            withTaskCount(Math.min(count, Runtime.getRuntime().availableProcessors())).
                            build()
            );

            for (int i = 1; i <= count; i++) {
                vertex1.addSourceFile(file);
            }

            vertex3.addSinkFile(target);

            DAG dag = new DAGImpl("testApplicationDag");

            dag.addVertex(vertex1);

            dag.addVertex(vertex2);

            dag.addVertex(vertex3);

            dag.addEdge(new EdgeImpl("edge1", vertex1, vertex2));

            dag.addEdge(new EdgeImpl(
                    "edge2",
                    vertex2,
                    vertex3,
                    true,
                    new ShufflingStrategyImpl(instance.getCluster().getLocalMember().getAddress()
                    )
            ));

            application.submit(dag);

            for (int i = 0; i < 1; i++) {
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
