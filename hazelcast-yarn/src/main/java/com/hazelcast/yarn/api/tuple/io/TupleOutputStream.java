package com.hazelcast.yarn.api.tuple.io;

import com.hazelcast.yarn.api.tuple.Tuple;
import com.hazelcast.yarn.api.actor.Consumer;

public interface TupleOutputStream<KeyOutPut, ValueOutPut> extends Consumer<Tuple<KeyOutPut, ValueOutPut>> {
    void consumeStream(TupleInputStream<KeyOutPut, ValueOutPut> inputStream) throws Exception;

    void consumeChunk(Tuple<KeyOutPut, ValueOutPut>[] chunk, int actualSize);

    boolean consume(Tuple<KeyOutPut, ValueOutPut> tuple) throws Exception;
}
