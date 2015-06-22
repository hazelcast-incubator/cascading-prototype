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

package com.hazelcast.jet.api.container;

public interface ContainerListener {
    ContainerListenerCaller EXECUTED_LISTENER_CALLER = new ContainerListenerCaller() {
        @Override
        public <T extends Throwable> void call(ContainerListener listener, T... payLoad) {
            listener.onContainerExecuted();
        }
    };

    ContainerListenerCaller INTERRUPTED_LISTENER_CALLER = new ContainerListenerCaller() {
        @Override
        public <T extends Throwable> void call(ContainerListener listener, T... payLoad) {
            listener.onContainerExecutionInterrupted();
        }
    };

    ContainerListenerCaller FAILURE_LISTENER_CALLER = new ContainerListenerCaller() {
        @Override
        public <T extends Throwable> void call(ContainerListener listener, T... payLoad) {
            listener.onContainerExecutionFailure(payLoad[0]);
        }
    };

    void onContainerExecuted();

    void onContainerExecutionInterrupted();

    void onContainerExecutionFailure(Throwable error);
}
