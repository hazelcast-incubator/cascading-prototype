package com.hazelcast.yarn.api.statemachine.container.applicationmaster;

import com.hazelcast.yarn.api.container.ContainerResponse;

public interface ApplicationMasterResponse extends ContainerResponse {
    ApplicationMasterResponse SUCCESS = new ApplicationMasterResponse() {
    };
    ApplicationMasterResponse FAILURE = new ApplicationMasterResponse() {
    };
}
