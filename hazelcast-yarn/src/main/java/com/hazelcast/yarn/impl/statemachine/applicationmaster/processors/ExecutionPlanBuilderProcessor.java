package com.hazelcast.yarn.impl.statemachine.applicationmaster.processors;

import java.util.Map;
import java.util.List;

import java.util.HashMap;
import java.util.Iterator;
import java.util.ArrayList;

import com.hazelcast.core.IFunction;
import com.hazelcast.spi.NodeEngine;

import java.lang.reflect.Constructor;
import java.util.concurrent.TimeUnit;

import com.hazelcast.yarn.api.dag.DAG;
import com.hazelcast.yarn.api.dag.Edge;
import com.hazelcast.yarn.api.dag.Vertex;
import com.hazelcast.yarn.api.tuple.TupleFactory;

import java.lang.reflect.InvocationTargetException;

import com.hazelcast.yarn.api.container.TupleChannel;
import com.hazelcast.yarn.impl.tuple.TupleFactoryImpl;
import com.hazelcast.yarn.api.container.TupleContainer;
import com.hazelcast.yarn.api.processor.ProcessorDescriptor;
import com.hazelcast.yarn.api.application.ApplicationContext;
import com.hazelcast.yarn.impl.container.DefaultTupleChannel;
import com.hazelcast.yarn.impl.container.DefaultTupleContainer;
import com.hazelcast.yarn.api.container.ContainerPayLoadProcessor;
import com.hazelcast.yarn.api.processor.TupleContainerProcessorFactory;
import com.hazelcast.yarn.api.container.applicationmaster.ApplicationMaster;
import com.hazelcast.yarn.impl.statemachine.container.requests.StartTupleContainerRequest;


import static com.hazelcast.util.Preconditions.checkNotNull;

public class ExecutionPlanBuilderProcessor implements ContainerPayLoadProcessor<DAG> {
    private final NodeEngine nodeEngine;
    private final TupleFactory tupleFactory;
    private final ApplicationMaster applicationMaster;
    private final ApplicationContext applicationContext;
    private final ClassLoader applicationClassLoader;

    private final IFunction<Vertex, TupleContainer> TUPLE_CONTAINER_CREATOR = new IFunction<Vertex, TupleContainer>() {
        @Override
        public TupleContainer apply(Vertex vertex) {
            ProcessorDescriptor descriptor = vertex.getDescriptor();
            String className = descriptor.getContainerProcessorClazz();
            TupleContainerProcessorFactory processorFactory = containerProcessorFactory(className);

            TupleContainer container = new DefaultTupleContainer(
                    vertex,
                    processorFactory,
                    nodeEngine,
                    applicationContext,
                    tupleFactory
            );

            applicationMaster.registerContainer(vertex, container);
            return container;
        }
    };

    public ExecutionPlanBuilderProcessor(ApplicationMaster applicationMaster) {
        this.applicationMaster = applicationMaster;
        this.nodeEngine = applicationMaster.getNodeEngine();
        this.applicationContext = applicationMaster.getApplicationContext();
        this.applicationClassLoader = this.applicationContext.getLocalizationStorage().getClassLoader();
        this.tupleFactory = new TupleFactoryImpl();
    }

    @Override
    public void process(DAG dag) throws Exception {
        checkNotNull(dag);

        //Process dag and container's chain building
        Iterator<Vertex> iterator = dag.getTopologicalVertexIterator();

        Map<Vertex, TupleContainer> vertex2ContainerMap = new HashMap<Vertex, TupleContainer>(dag.getVertices().size());

        List<TupleContainer> roots = new ArrayList<TupleContainer>();

        while (iterator.hasNext()) {
            Vertex vertex = iterator.next();
            List<Edge> edges = vertex.getInputEdges();
            TupleContainer next = TUPLE_CONTAINER_CREATOR.apply(vertex);
            vertex2ContainerMap.put(vertex, next);

            if (edges.size() == 0) {
                roots.add(next);
            } else {
                for (Edge edge : edges) {
                    join(vertex2ContainerMap.get(edge.getInputVertex()), edge, next);
                }
            }

//            if (vertex.getOutputEdges().size() == 0) {
//                applicationMaster.registerLeafContainer(next);
//            }
        }

        long secondsToAwait = this.nodeEngine.getConfig().getYarnApplicationConfig(applicationContext.getName()).getApplicationSecondsToAwait();

        for (TupleContainer container : roots) {
            applicationMaster.addFollower(container);
        }

        this.applicationContext.getProcessingExecutor().startWorkers();

        for (TupleContainer container : this.applicationMaster.containers()) {
            container.handleContainerRequest(new StartTupleContainerRequest()).get(secondsToAwait, TimeUnit.SECONDS);
        }
    }

    private TupleContainer join(TupleContainer container, Edge edge, TupleContainer nextContainer) {
        this.linkWithChannel(container, nextContainer, edge);
        return nextContainer;
    }

    private void linkWithChannel(TupleContainer container,
                                 TupleContainer next,
                                 Edge edge
    ) {
        if ((container != null) && (next != null)) {
            TupleChannel channel = new DefaultTupleChannel(container, next, edge);

            container.addFollower(next);
            next.addPredecessor(container);

            container.addOutputChannel(channel);
            next.addInputChannel(channel);
        }
    }

    private TupleContainerProcessorFactory containerProcessorFactory(String className) {
        try {
            Class<TupleContainerProcessorFactory> clazz = (Class<TupleContainerProcessorFactory>) Class.forName(className, true, this.applicationClassLoader);
            Constructor<TupleContainerProcessorFactory> constructor = clazz.getConstructor();
            return constructor.newInstance();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }
}
