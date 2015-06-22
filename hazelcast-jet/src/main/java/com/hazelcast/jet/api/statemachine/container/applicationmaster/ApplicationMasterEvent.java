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

package com.hazelcast.jet.api.statemachine.container.applicationmaster;

import com.hazelcast.jet.api.statemachine.container.ContainerEvent;

public enum ApplicationMasterEvent implements ContainerEvent {
    SUBMIT_DAG,
    EXECUTION_PLAN_BUILD_FAILED,
    EXECUTION_PLAN_READY,
    EXECUTE,
    INVALIDATE,
    INTERRUPT_EXECUTION,
    EXECUTION_INTERRUPTED,
    INTERRUPTION_FAILURE,
    EXECUTION_ERROR,
    EXECUTION_COMPLETED,
    FINALIZE,
}
