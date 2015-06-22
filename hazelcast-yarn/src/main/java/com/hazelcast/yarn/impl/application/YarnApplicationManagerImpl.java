package com.hazelcast.yarn.impl.application;

import com.hazelcast.core.IFunction;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.IConcurrentMap;
import com.hazelcast.yarn.api.YarnApplicationManager;
import com.hazelcast.util.SampleableConcurrentHashMap;
import com.hazelcast.yarn.api.application.ApplicationContext;

public class YarnApplicationManagerImpl implements YarnApplicationManager {
    private final IConcurrentMap<String, ApplicationContext> applicationContexts = new SampleableConcurrentHashMap<String, ApplicationContext>(16);

    private final NodeEngine nodeEngine;

    private final IFunction<String, ApplicationContext> function = new IFunction<String, ApplicationContext>() {
        @Override
        public ApplicationContext apply(String name) {
            return new ApplicationContextImpl(name, nodeEngine);
        }
    };

    public YarnApplicationManagerImpl(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    @Override
    public ApplicationContext createOrGetApplicationContext(String name) {
        return this.applicationContexts.applyIfAbsent(name, function);
    }

    @Override
    public ApplicationContext getApplicationContext(String name) {
        return this.applicationContexts.get(name);
    }

    @Override
    public void destroyApplication(String name) {
        this.applicationContexts.remove(name);
    }
}
