package com.hazelcast.yarn.impl.tuple;

import java.util.Iterator;

import com.hazelcast.yarn.api.tuple.Tuple;
import com.hazelcast.yarn.api.tuple.TupleConvertor;
import com.hazelcast.internal.serialization.SerializationService;

public class TupleIterator<R, K, V> implements Iterator<Tuple<K, V>> {
    private final Iterator<R> iterator;
    private final TupleConvertor<R, K, V> convertor;
    private final SerializationService serializationService;

    public TupleIterator(Iterator<R> iterator, TupleConvertor<R, K, V> convertor, SerializationService serializationService) {
        this.iterator = iterator;
        this.convertor = convertor;
        this.serializationService = serializationService;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    public void remove() {
        throw new IllegalStateException("not supported exception");
    }

    @Override
    public Tuple<K, V> next() {
        return convertor.convert(iterator.next(), serializationService);
    }
}
