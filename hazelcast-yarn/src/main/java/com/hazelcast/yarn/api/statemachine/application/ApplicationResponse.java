package com.hazelcast.yarn.api.statemachine.application;

import com.hazelcast.yarn.api.statemachine.StateMachineOutput;

public interface ApplicationResponse extends StateMachineOutput{
    ApplicationResponse SUCCESS = new ApplicationResponse() {
    };
    ApplicationResponse FAILURE = new ApplicationResponse() {
    };
}
