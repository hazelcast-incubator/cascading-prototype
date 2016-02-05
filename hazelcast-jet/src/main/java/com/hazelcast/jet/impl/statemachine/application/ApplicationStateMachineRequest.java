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

package com.hazelcast.jet.impl.statemachine.application;

import com.hazelcast.jet.api.Dummy;
import com.hazelcast.jet.api.statemachine.StateMachineRequest;
import com.hazelcast.jet.api.statemachine.application.ApplicationEvent;

public class ApplicationStateMachineRequest implements StateMachineRequest<ApplicationEvent, Dummy> {
    private final ApplicationEvent event;

    public ApplicationStateMachineRequest(ApplicationEvent event) {
        this.event = event;
    }

    @Override
    public ApplicationEvent getContainerEvent() {
        return this.event;
    }

    @Override
    public Dummy getPayLoad() {
        return Dummy.INSTANCE;
    }
}
