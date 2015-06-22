package com.hazelcast.yarn.api.tuple.io;

import java.util.Iterator;
import com.hazelcast.yarn.api.tuple.Tuple;

public interface TupleInputStream<KeyInput, ValueInput> extends ProducerInputStream<Tuple<KeyInput, ValueInput>>, Iterable<Tuple<KeyInput, ValueInput>> {
    Tuple<KeyInput, ValueInput> get(int idx);

    Iterator<Tuple<KeyInput, ValueInput>> iterator();
}
