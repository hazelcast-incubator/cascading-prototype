package com.hazelcast.jet.countertest;


import com.hazelcast.jet.api.container.CounterKey;
import org.junit.Test;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.experimental.categories.Category;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;

import com.hazelcast.core.IMap;

import java.util.concurrent.Future;

import com.hazelcast.config.Config;
import com.hazelcast.jet.api.dag.DAG;
import com.hazelcast.jet.api.dag.Vertex;
import com.hazelcast.jet.impl.dag.DAGImpl;
import com.hazelcast.jet.impl.dag.VertexImpl;

import java.util.concurrent.ExecutionException;

import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.jet.TestJetHazelcastFactory;
import com.hazelcast.jet.api.counters.Accumulator;
import com.hazelcast.jet.impl.counters.LongCounter;
import com.hazelcast.jet.api.application.Application;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.jet.api.config.JetApplicationConfig;
import com.hazelcast.jet.api.processor.ProcessorDescriptor;
import com.hazelcast.jet.api.hazelcast.JetHazelcastInstance;
import com.hazelcast.jet.impl.hazelcast.client.JetHazelcastClientProxy;


import static org.junit.Assert.assertEquals;


@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class})
public class CounterTest {
    private final TestJetHazelcastFactory hazelcastFactory = new TestJetHazelcastFactory();

    private JetHazelcastInstance server;
    private JetHazelcastClientProxy client;

    @Before
    public void setup() {
        Config config = new Config();
        JetApplicationConfig jetConfig = new JetApplicationConfig("testApplication");
        jetConfig.setApplicationSecondsToAwait(100000);
        jetConfig.setJetSecondsToAwait(100000);
        jetConfig.setChunkSize(4000);
        jetConfig.setMaxProcessingThreads(
                Runtime.getRuntime().availableProcessors());
        config.addCustomServiceConfig(jetConfig);

        this.server = this.hazelcastFactory.newHazelcastInstance(config);
        this.client = (JetHazelcastClientProxy) this.hazelcastFactory.newHazelcastClient(null);
        this.client.getClientConfig().addCustomServiceConfig(jetConfig);
    }

    @Test
    public void serverTest() throws ExecutionException, InterruptedException {
        run(
                this.server
        );
    }

    @Test
    public void clientTest() throws ExecutionException, InterruptedException {
        run(
                this.client
        );
    }

    private void run(JetHazelcastInstance instance) throws ExecutionException, InterruptedException {
        final IMap<Integer, String> sourceMap = instance.getMap("source");
        final IMap<Integer, String> targetMap = instance.getMap("target");

        int CNT = 1000;

        List<Future> l = new ArrayList<Future>();

        for (int i = 1; i <= CNT; i++) {
            l.add(sourceMap.putAsync(i, String.valueOf(i)));
        }

        System.out.println("Waiting ....");

        for (Future f : l) {
            f.get();
        }

        System.out.println("Running .... " + sourceMap.size());

        try {
            Application application = instance.getJetApplication("testApplication");

            Vertex vertex = new VertexImpl(
                    "mod1",
                    ProcessorDescriptor.
                            builder(CounterProcessor.CounterProcessorFactory.class).
                            withTaskCount(1).
                            build()
            );

            vertex.addSourceMap("source");
            vertex.addSinkMap("target");
            DAG dag = new DAGImpl("testApplicationDag");
            dag.addVertex(vertex);
            application.submit(dag);

            application.execute().get();

            Map<CounterKey, Accumulator> accumulatorMap = application.getAccumulators();
            LongCounter longCounter = (LongCounter) accumulatorMap.values().iterator().next();

            assertEquals(longCounter.getPrimitiveValue(), CNT);
            application.finalizeApplication();
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

    @After
    public void tearDown() {
        this.hazelcastFactory.terminateAll();
    }
}
