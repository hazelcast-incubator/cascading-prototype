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

package com.hazelcast.jet.impl.statemachine.applicationmaster.processors;

import java.util.concurrent.BlockingQueue;

import com.hazelcast.jet.api.container.DataContainer;
import com.hazelcast.jet.api.container.ContainerPayLoadProcessor;
import com.hazelcast.jet.api.container.applicationmaster.ApplicationMaster;

public class InvalidateApplicationProcessor implements ContainerPayLoadProcessor<Throwable> {
    private final ApplicationMaster applicationMaster;

    public InvalidateApplicationProcessor(ApplicationMaster applicationMaster) {
        this.applicationMaster = applicationMaster;
    }

    @Override
    public void process(Throwable error) throws Exception {
        try {
            for (DataContainer container : this.applicationMaster.containers()) {
                container.invalidate();
            }
        } finally {
            BlockingQueue<Object> executionMailBox =
                    this.applicationMaster.getExecutionMailBox();

            if (executionMailBox != null) {
                executionMailBox.offer(error);
            }
        }
    }
}
