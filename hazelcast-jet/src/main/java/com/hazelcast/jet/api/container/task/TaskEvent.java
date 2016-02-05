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

package com.hazelcast.jet.api.container.task;

/**
 * Represents task's events
 */
public enum TaskEvent {
    /**
     * Raised on tasks' completion;
     */
    TASK_EXECUTION_COMPLETED,
    /**
     * Raised when task is ready for the finalization phase;
     */
    TASK_READY_FOR_FINALIZATION,
    /**
     * Raised on execution's error;
     */
    TASK_EXECUTION_ERROR,
    /**
     * Raised on task's interruption;
     */
    TASK_INTERRUPTED
}
