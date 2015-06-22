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

package com.hazelcast.jet.api.application;

import java.util.Set;
import java.util.concurrent.Callable;

import com.hazelcast.core.Member;
import com.hazelcast.jet.impl.Chunk;
import com.hazelcast.jet.api.config.JetApplicationConfig;
import com.hazelcast.jet.api.hazelcast.InvocationFactory;
import com.hazelcast.jet.api.statemachine.application.ApplicationEvent;

public interface ApplicationInvocationService<PayLoad> {
    Set<Member> getMembers();

    PayLoad createInterruptInvoker();

    PayLoad createExecutionInvoker();

    PayLoad createAccumulatorsInvoker();

    PayLoad createFinalizationInvoker();

    PayLoad createAcceptedLocalizationInvoker();

    PayLoad createLocalizationInvoker(Chunk chunk);

    PayLoad createEventInvoker(ApplicationEvent applicationEvent);

    PayLoad createInitApplicationInvoker(JetApplicationConfig config);

    <T> Callable<T> createInvocation(Member member,
                                     InvocationFactory<PayLoad> operationFactory);
}
