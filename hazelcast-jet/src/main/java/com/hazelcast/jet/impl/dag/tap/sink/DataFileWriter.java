/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.dag.tap.sink;

import com.hazelcast.jet.api.dag.tap.SinkTap;
import com.hazelcast.jet.api.data.tuple.Tuple;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.jet.api.dag.tap.SinkOutputStream;
import com.hazelcast.jet.api.dag.tap.SinkTapWriteStrategy;
import com.hazelcast.jet.api.container.ContainerContext;
import com.hazelcast.jet.api.data.io.ProducerInputStream;
import com.hazelcast.jet.api.application.ApplicationContext;
import com.hazelcast.jet.api.application.ApplicationListener;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;

public class DataFileWriter extends AbstractHazelcastWriter {
    private final SinkOutputStream sinkOutputStream;

    private boolean closed = true;

    public DataFileWriter(ContainerContext containerContext,
                          int partitionID,
                          SinkTap tap) {
        super(containerContext, partitionID, SinkTapWriteStrategy.CLEAR_AND_REPLACE);
        this.sinkOutputStream = tap.getSinkOutputStream();

        containerContext.getApplicationContext().registerApplicationListener(new ApplicationListener() {
            @Override
            public void onApplicationExecuted(ApplicationContext applicationContext) {
                closeFile();
            }
        });
    }

    @Override
    protected void processChunk(ProducerInputStream stream) {
        checkFileOpen();

        StringBuilder sb = new StringBuilder();

        for (Object o : stream) {
            Tuple t = (Tuple) o;

            for (int i = 0; i < t.keySize(); i++) {
                sb.append(t.getKey(i).toString()).append(" ");
            }

            for (int i = 0; i < t.valueSize(); i++) {
                sb.append(t.getValue(i).toString()).append(" ");
            }

            sb.append("\r\n");
        }

        if (sb.length() > 0) {
            this.sinkOutputStream.write(sb.toString());
            this.sinkOutputStream.flush();
        }
    }

    private void checkFileOpen() {
        if (this.closed) {
            this.sinkOutputStream.open();
            this.closed = false;
        }
    }

    private void closeFile() {
        if (!this.closed) {
            this.sinkOutputStream.close();
            this.closed = true;
        }
    }

    @Override
    public PartitioningStrategy getPartitionStrategy() {
        return StringPartitioningStrategy.INSTANCE;
    }

    @Override
    public boolean isPartitioned() {
        return false;
    }
}

