package com.hazelcast.jet.base;


import org.junit.AfterClass;


import java.io.File;
import java.util.List;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.io.InputStream;
import java.io.IOException;
import java.io.FileInputStream;

import com.hazelcast.core.IMap;

import java.io.LineNumberReader;
import java.io.BufferedInputStream;
import java.util.concurrent.Future;


import com.hazelcast.jet.api.dag.DAG;
import com.hazelcast.jet.api.dag.Edge;
import com.hazelcast.jet.api.dag.Vertex;
import com.hazelcast.jet.impl.dag.DAGImpl;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.jet.impl.dag.VertexImpl;
import com.hazelcast.jet.api.config.JetConfig;
import com.hazelcast.test.HazelcastTestSupport;

import java.util.concurrent.atomic.AtomicInteger;

import com.hazelcast.jet.api.application.Application;
import com.hazelcast.jet.api.config.JetApplicationConfig;
import com.hazelcast.jet.api.processor.ProcessorDescriptor;
import com.hazelcast.jet.api.hazelcast.JetHazelcastInstance;
import com.hazelcast.jet.impl.hazelcast.client.JetHazelcastClientProxy;

public abstract class JetBaseTest extends HazelcastTestSupport {
    public static final int TIME_TO_AWAIT = 600;
    public static final String TEST_DATA_PATH = "test.data.path";

    protected static JetApplicationConfig JETCONFIG;
    protected static JetConfig CONFIG;
    protected static JetHazelcastInstance SERVER;
    protected static JetHazelcastClientProxy CLIENT;
    protected static List<File> createdFiles = new ArrayList<File>();
    protected static TestJetHazelcastFactory HAZELCAST_FACTORY;
    protected static JetHazelcastInstance[] HAZELCAST_INSTANCES;

    private static final AtomicInteger APPLICATION_NAME_COUNTER = new AtomicInteger();

    public static void initCluster(int membersCount) throws Exception {
        JETCONFIG = new JetApplicationConfig("testApplication");
        JETCONFIG.setJetSecondsToAwait(100000);
        JETCONFIG.setChunkSize(4000);
        JETCONFIG.setMaxProcessingThreads(
                Runtime.getRuntime().availableProcessors());

        System.setProperty(TEST_DATA_PATH, "src/test/resources/data/");

        CONFIG = new JetConfig();
        CONFIG.addJetApplicationConfig(JETCONFIG);
        CONFIG.getMapConfig("source").setInMemoryFormat(InMemoryFormat.OBJECT);

        HAZELCAST_FACTORY = new TestJetHazelcastFactory();

        buildCluster(membersCount);
        warmUpPartitions(SERVER);

        CLIENT.getClientConfig().addJetApplicationConfig(JETCONFIG);
    }

    protected Application createApplication() {
        return createApplication("testApplication " + APPLICATION_NAME_COUNTER.incrementAndGet());
    }

    protected Application createApplication(String applicationName) {
        return SERVER.getJetApplication(applicationName, JETCONFIG);
    }

    protected DAG createDAG() {
        return createDAG("testDagApplication");
    }

    protected DAG createDAG(String dagName) {
        return new DAGImpl(dagName);
    }

    protected static void buildCluster(int memberCount) {
        HAZELCAST_INSTANCES = new JetHazelcastInstance[memberCount];

        for (int i = 0; i < memberCount; i++) {
            HAZELCAST_INSTANCES[i] = HAZELCAST_FACTORY.newHazelcastInstance(CONFIG);
        }

        SERVER = HAZELCAST_INSTANCES[0];
        CLIENT = (JetHazelcastClientProxy) HAZELCAST_FACTORY.newHazelcastClient(null);
    }

    protected void fillMap(String source, JetHazelcastInstance instance, int CNT) throws Exception {
        final IMap<Integer, String> sourceMap = instance.getMap(source);

        List<Future> l = new ArrayList<Future>();

        for (int i = 1; i <= CNT; i++) {
            l.add(sourceMap.putAsync(i, String.valueOf(i)));
        }

        for (Future f : l) {
            f.get();
        }
    }

    protected Vertex createVertex(String name, Class processorClass, int processorsCount) {
        return new VertexImpl(
                name,
                ProcessorDescriptor.
                        builder(processorClass).
                        withTaskCount(processorsCount).
                        build()
        );
    }

    protected Vertex createVertex(String name, Class processorClass) {
        return createVertex(name, processorClass, Runtime.getRuntime().availableProcessors());
    }

    public Future executeApplication(DAG dag, Application application) throws Exception {
        application.submit(dag);
        return application.execute();
    }

    protected void addVertices(DAG dag, Vertex... vertices) {
        for (Vertex vertex : vertices) {
            dag.addVertex(vertex);
        }
    }

    protected void addEdges(DAG dag, Edge edge) {
        dag.addEdge(edge);
    }

    protected String createDataFile(int recordsCount, String file) throws Exception {
        String inputPath = System.getProperty(TEST_DATA_PATH);

        File f = new File(inputPath + file);
        this.createdFiles.add(f);

        FileWriter fw = new FileWriter(f);
        StringBuilder sb = new StringBuilder();

        for (int i = 1; i <= recordsCount; i++) {
            sb.append(String.valueOf(i)).append(" \n");

            if (i % 4096 == 0) {
                fw.write(sb.toString());
                fw.flush();
                sb = new StringBuilder();
            }
        }

        if (sb.length() > 0) {
            fw.write(sb.toString());
            fw.flush();
            fw.close();
        }

        return f.getAbsolutePath();
    }

    protected String touchFile(String file) {
        String inputPath = System.getProperty(TEST_DATA_PATH);
        File f = new File(inputPath + file);
        this.createdFiles.add(f);
        return f.getAbsolutePath();
    }

    protected void fillMapWithDataFromFile(JetHazelcastInstance hazelcastInstance, String mapName, String file)
            throws Exception {
        IMap<String, String> map = hazelcastInstance.getMap(mapName);
        String inputPath = System.getProperty(TEST_DATA_PATH);

        LineNumberReader reader = new LineNumberReader(
                new FileReader(new File(inputPath + file))
        );

        try {
            String line;
            List<Future> futures = new ArrayList<Future>();
            StringBuilder sb = new StringBuilder();

            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }

            futures.add(map.putAsync(file, sb.toString()));

            for (Future ff : futures) {
                ff.get();
            }
        } finally {
            reader.close();
        }
    }

    protected long fileLinesCount(String sinkFile) throws IOException {
        InputStream is = new BufferedInputStream(new FileInputStream(sinkFile));
        try {
            byte[] c = new byte[1024];

            long count = 0;
            int readChars;
            boolean empty = true;

            while ((readChars = is.read(c)) != -1) {
                empty = false;
                for (int i = 0; i < readChars; ++i) {
                    if (c[i] == '\n') {
                        ++count;
                    }
                }
            }
            return (count == 0 && !empty) ? 1 : count;
        } finally {
            is.close();
        }
    }

    @AfterClass
    public static void after() {
        try {
            HAZELCAST_FACTORY.terminateAll();
        } finally {
            try {
                for (File f : createdFiles) {
                    if (f != null) {
                        f.delete();
                    }
                }
            } finally {
                createdFiles.clear();
            }
        }
    }

    protected void printException(Exception e) {
        e.printStackTrace(System.out);
    }
}
