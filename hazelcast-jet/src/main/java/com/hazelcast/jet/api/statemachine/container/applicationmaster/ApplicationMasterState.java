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

import com.hazelcast.jet.api.statemachine.container.ContainerState;

public enum ApplicationMasterState implements ContainerState {
    NEW,
    DAG_SUBMITTED,
    READY_FOR_EXECUTION,
    INVALID_DAG_FOR_EXECUTION,
    EXECUTING,
    EXECUTION_INTERRUPTING,
    EXECUTION_INTERRUPTED,
    EXECUTION_FAILED,
    EXECUTION_SUCCESS,
    INVALIDATED,
    FINALIZED
}
