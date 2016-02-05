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

package com.hazelcast.jet.api.container;

/***
 * Interface which represents processor to be invoked on container's state-machine work;
 *
 * @param <PayLoad> - type of the processor's argument;
 */
public interface ContainerPayLoadProcessor<PayLoad> {
    /**
     * Invoked on change of state of container's state-machine;
     *
     * @param payload - argument;
     * @throws Exception
     */
    void process(PayLoad payload) throws Exception;
}
