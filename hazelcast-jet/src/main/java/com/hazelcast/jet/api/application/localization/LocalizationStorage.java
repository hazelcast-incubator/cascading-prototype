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

package com.hazelcast.jet.api.application.localization;

import java.util.Map;
import java.io.IOException;

import com.hazelcast.jet.impl.Chunk;
import com.hazelcast.jet.api.JetException;
import com.hazelcast.jet.impl.application.LocalizationResourceDescriptor;
import com.hazelcast.jet.impl.application.localization.classloader.ResourceStream;


public interface LocalizationStorage {
    void receiveFileChunk(Chunk chunk) throws IOException, JetException;

    void accept() throws InvalidLocalizationException;

    ClassLoader getClassLoader();

    Map<LocalizationResourceDescriptor, ResourceStream> getResources() throws IOException;
}
