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

package com.hazelcast.jet.impl.data.io.writers;

import com.hazelcast.jet.impl.data.io.Types;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.jet.spi.data.io.ObjectWriter;
import com.hazelcast.jet.spi.data.io.ObjectWriterFactory;


public class ShortWriter implements ObjectWriter<Short> {
    @Override
    public void writeType(Short object,
                          ObjectDataOutput objectDataOutput,
                          ObjectWriterFactory objectWriterFactory) throws Exception {
        objectDataOutput.write(Types.SHORT.getTypeID());
    }

    @Override
    public void writePayLoad(Short object,
                             ObjectDataOutput objectDataOutput,
                             ObjectWriterFactory objectWriterFactory) throws Exception {
        objectDataOutput.writeShort(object);
    }

    @Override
    public void write(Short object,
                      ObjectDataOutput objectDataOutput,
                      ObjectWriterFactory objectWriterFactory) throws Exception {
        writeType(object, objectDataOutput, objectWriterFactory);
        writePayLoad(object, objectDataOutput, objectWriterFactory);
    }
}
