/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.stream.list;


import java.util.Comparator;
import java.util.List;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Optional;

import com.hazelcast.core.IList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToDoubleFunction;


import java.util.function.BinaryOperator;

import java.util.function.BiFunction;

import java.util.function.Supplier;
import java.util.function.BiConsumer;


import java.util.function.IntFunction;


import java.util.function.Consumer;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.Collector;


import com.hazelcast.jet.spi.dag.DAG;
import com.hazelcast.jet.spi.dag.Edge;

import java.util.function.ToIntFunction;

import com.hazelcast.jet.spi.dag.Vertex;
import processors.MapContainerProcessor;


import processors.SortContainerProcessor;

import java.util.function.ToLongFunction;

import com.hazelcast.jet.impl.dag.DAGImpl;
import com.hazelcast.jet.impl.dag.EdgeImpl;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.impl.dag.VertexImpl;
import com.hazelcast.jet.spi.config.JetConfig;

import java.util.concurrent.ExecutionException;

import com.hazelcast.jet.impl.hazelcast.JetEngine;
import com.hazelcast.jet.api.application.Application;
import com.hazelcast.jet.spi.config.JetApplicationConfig;
import com.hazelcast.jet.spi.processor.ProcessorDescriptor;


//CHECKSTYLE:OFF
public class IListStreamImpl<T> implements Stream<T> {
    private final String listName;
    private final List<Object> arguments;
    private final List<StreamOperators> operators;
    private final HazelcastInstance jetHazelcastInstance;

    public IListStreamImpl(HazelcastInstance jetHazelcastInstance, String listName) {
        this.listName = listName;
        this.operators = new ArrayList<StreamOperators>();
        this.arguments = new ArrayList<Object>();
        this.jetHazelcastInstance = (HazelcastInstance) jetHazelcastInstance;
    }

    private IListStreamImpl(HazelcastInstance jetHazelcastInstance, String listName, List<Object> arguments, List<StreamOperators> operators) {
        this.listName = listName;
        this.operators = operators;
        this.arguments = arguments;
        this.jetHazelcastInstance = (HazelcastInstance) jetHazelcastInstance;
    }

    @Override
    public Stream<T> filter(Predicate<? super T> predicate) {
        operators.add(StreamOperators.FILTER);
        arguments.add(predicate);

        return this;
    }

    @Override
    public <R> Stream<R> map(Function<? super T, ? extends R> mapper) {
        operators.add(StreamOperators.MAP);
        arguments.add(mapper);

        return new IListStreamImpl<R>(this.jetHazelcastInstance, this.listName, this.arguments, this.operators);
    }

    @Override
    public Stream<T> sorted() {
        operators.add(StreamOperators.SORT);
        arguments.add(null);
        return this;
    }

    @Override
    public Stream<T> sorted(Comparator<? super T> comparator) {
        operators.add(StreamOperators.SORT);
        arguments.add(comparator);
        return this;
    }

    @Override
    public IntStream mapToInt(ToIntFunction<? super T> mapper) {
        return null;
    }

    @Override
    public LongStream mapToLong(ToLongFunction<? super T> mapper) {
        return null;
    }

    @Override
    public DoubleStream mapToDouble(ToDoubleFunction<? super T> mapper) {
        return null;
    }

    @Override
    public <R> Stream<R> flatMap(Function<? super T, ? extends Stream<? extends R>> mapper) {
        return null;
    }

    @Override
    public IntStream flatMapToInt(Function<? super T, ? extends IntStream> mapper) {
        return null;
    }

    @Override
    public LongStream flatMapToLong(Function<? super T, ? extends LongStream> mapper) {
        return null;
    }

    @Override
    public DoubleStream flatMapToDouble(Function<? super T, ? extends DoubleStream> mapper) {
        return null;
    }

    @Override
    public Stream<T> distinct() {
        return null;
    }


    @Override
    public Stream<T> peek(Consumer<? super T> action) {
        return null;
    }

    @Override
    public Stream<T> limit(long maxSize) {
        return null;
    }

    @Override
    public Stream<T> skip(long n) {
        return null;
    }

    private Vertex toDag(List<StreamOperators> operators,
                         List<Object> arguments,
                         DAG dag,
                         int operatorIndex,
                         Vertex lastVertex,
                         Class containerProcessorFactoryClazz
    ) {
        ProcessorDescriptor processorDescriptor = ProcessorDescriptor.builder(
                containerProcessorFactoryClazz,
                arguments.toArray(new Object[arguments.size()])
        ).build();

        Vertex vertex = new VertexImpl(
                "vertex " + operatorIndex,
                processorDescriptor
        );

        dag.addVertex(vertex);

        if (lastVertex != null) {
            this.join(lastVertex, vertex, dag);
        }

        operators.clear();
        arguments.clear();

        return vertex;
    }

    private String execute() {
        Application application = null;
        String sinkList = "hz_list_" + System.currentTimeMillis();

        try {
            DAG dag = new DAGImpl();

            List<Object> mapArguments = new ArrayList<Object>();
            List<Object> sortArguments = new ArrayList<Object>();

            List<StreamOperators> mapOperators = new ArrayList<StreamOperators>();
            List<StreamOperators> sortOperators = new ArrayList<StreamOperators>();

            int operatorIndex = 0;

            Vertex lastVertex = null;
            Vertex firstVertex = null;

            for (int idx = 0; idx < this.operators.size(); idx++) {
                operatorIndex++;

                StreamOperators operator = this.operators.get(idx);
                Object argument = this.arguments.get(idx);

                if ((operator == StreamOperators.MAP) || (operator == StreamOperators.FILTER)) {
                    mapOperators.add(operator);
                    mapArguments.add(argument);

                    if (sortOperators.size() > 0) {
                        lastVertex = this.toDag(sortOperators, sortArguments, dag, operatorIndex, lastVertex, SortContainerProcessor.SortContainerProcessorFactory.class);
                    }
                }

                if (operator == StreamOperators.SORT) {
                    sortOperators.add(operator);
                    if (argument != null) {
                        sortArguments.add(argument);
                    }

                    if (mapOperators.size() > 0) {
                        lastVertex = this.toDag(mapOperators, mapArguments, dag, operatorIndex, lastVertex, MapContainerProcessor.MapContainerProcessorFactory.class);
                    }
                }

                if (firstVertex == null) {
                    firstVertex = lastVertex;
                }
            }

            operatorIndex++;

            if (sortOperators.size() > 0) {
                lastVertex = this.toDag(sortOperators, sortArguments, dag, operatorIndex, lastVertex, SortContainerProcessor.SortContainerProcessorFactory.class);
            } else if (mapOperators.size() > 0) {
                lastVertex = this.toDag(mapOperators, mapArguments, dag, operatorIndex, lastVertex, MapContainerProcessor.MapContainerProcessorFactory.class);
            }

            if (firstVertex == null) {
                firstVertex = lastVertex;
            }

            if (firstVertex == null) {
                return null;
            }

            firstVertex.addSourceList(this.listName);
            lastVertex.addSinkList(sinkList);

            String applicationName = "hz_sa_" + System.currentTimeMillis();

            JetApplicationConfig config = new JetApplicationConfig(applicationName);
            ((JetConfig) this.jetHazelcastInstance.getConfig()).addJetApplicationConfig(config);
            config.setJetSecondsToAwait(100000);
            config.setChunkSize(4096);
            config.setContainerQueueSize(65536 * 8);
            config.setMaxProcessingThreads(Runtime.getRuntime().availableProcessors());

            application = JetEngine.getJetApplication(this.jetHazelcastInstance, applicationName);
            application.submit(dag);
            application.execute().get();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } finally {
            if (application != null) {
                application.finalizeApplication();
            }
        }

        return sinkList;
    }

    private void join(Vertex lastVertex, Vertex v, DAG dag) {
        Edge edge = new EdgeImpl.
                EdgeBuilder("edge", lastVertex, v).
                build();

        dag.addEdge(edge);
    }

    @Override
    public void forEach(Consumer<? super T> action) {
        String resultList = this.execute();
        IList<T> result = this.jetHazelcastInstance.getList(resultList);

        for (T object : result) {
            action.accept(object);
        }
    }

    @Override
    public void forEachOrdered(Consumer<? super T> action) {
        //Execute
    }

    @Override
    public Object[] toArray() {
        return new Object[0];
    }

    @Override
    public <A> A[] toArray(IntFunction<A[]> generator) {
        return null;
    }

    @Override
    public T reduce(T identity, BinaryOperator<T> accumulator) {
        return null;
    }

    @Override
    public Optional<T> reduce(BinaryOperator<T> accumulator) {
        return null;
    }

    @Override
    public <U> U reduce(U identity, BiFunction<U, ? super T, U> accumulator, BinaryOperator<U> combiner) {
        return null;
    }

    @Override
    public <R> R collect(Supplier<R> supplier, BiConsumer<R, ? super T> accumulator, BiConsumer<R, R> combiner) {
        return null;
    }

    @Override
    public <R, A> R collect(Collector<? super T, A, R> collector) {
        return null;
    }

    @Override
    public Optional<T> min(Comparator<? super T> comparator) {
        //Execute
        return null;
    }

    @Override
    public Optional<T> max(Comparator<? super T> comparator) {
        //Execute
        return null;
    }

    @Override
    public long count() {
        //Execute
        return 0;
    }

    @Override
    public boolean anyMatch(Predicate<? super T> predicate) {
        //Execute
        return false;
    }

    @Override
    public boolean allMatch(Predicate<? super T> predicate) {
        //Execute
        return false;
    }

    @Override
    public boolean noneMatch(Predicate<? super T> predicate) {
        //Execute
        return false;
    }

    @Override
    public Optional<T> findFirst() {
        //Execute
        return null;
    }

    @Override
    public Optional<T> findAny() {
        //Execute
        return null;
    }

    @Override
    public Iterator<T> iterator() {
        //Execute
        return null;
    }

    @Override
    public Spliterator<T> spliterator() {
        //Execute
        return null;
    }

    @Override
    public boolean isParallel() {
        return false;
    }

    @Override
    public Stream<T> sequential() {
        return null;
    }

    @Override
    public Stream<T> parallel() {
        return null;
    }

    @Override
    public Stream<T> unordered() {
        return null;
    }

    @Override
    public Stream<T> onClose(Runnable closeHandler) {
        return null;
    }

    @Override
    public void close() {

    }
}
//CHECKSTYLE:ON
