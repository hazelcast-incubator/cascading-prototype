package com.hazelcast.jet.base;

import java.util.List;


import java.util.ArrayList;
import java.util.Collection;


import com.hazelcast.nio.Address;

import java.net.InetSocketAddress;

import com.hazelcast.test.TestEnvironment;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.util.AddressHelper;
import com.hazelcast.client.config.ClientConfig;

import java.util.concurrent.atomic.AtomicInteger;

import com.hazelcast.jet.api.config.JetClientConfig;
import com.hazelcast.client.connection.AddressProvider;

import com.hazelcast.instance.OutOfMemoryErrorDispatcher;

import com.hazelcast.jet.api.hazelcast.JetHazelcastInstance;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.ClientConnectionManagerFactory;
import com.hazelcast.jet.impl.hazelcast.client.JetHazelcastClient;
import com.hazelcast.jet.impl.hazelcast.client.JetHazelcastClientProxy;
import com.hazelcast.jet.impl.hazelcast.client.JetHazelcastClientInstanceImpl;


public class TestJetHazelcastFactory extends TestJetHazelcastInstanceFactory {

    private static final AtomicInteger clientPorts = new AtomicInteger(6000);
    private final boolean mockNetwork = TestEnvironment.isMockNetwork();
    private final List<HazelcastClientInstanceImpl> clients = new ArrayList<HazelcastClientInstanceImpl>(10);
    private final TestClientRegistry clientRegistry;

    public TestJetHazelcastFactory() {
        super(0);
        this.clientRegistry = new TestClientRegistry(registry);
    }

    public JetHazelcastInstance newHazelcastClient() {
        return this.newHazelcastClient(null);
    }

    public JetHazelcastInstance newHazelcastClient(JetClientConfig config) {
        if (!mockNetwork) {
            return JetHazelcastClient.newHazelcastClient(config);
        }

        if (config == null) {
            config = new JetClientConfig();
        }

        final ClassLoader tccl = Thread.currentThread().getContextClassLoader();
        JetHazelcastClientProxy proxy;

        try {
            Thread.currentThread().setContextClassLoader(HazelcastClient.class.getClassLoader());
            ClientConnectionManagerFactory clientConnectionManagerFactory =
                    clientRegistry.createClientServiceFactory(createAddress("127.0.0.1", clientPorts.incrementAndGet()));
            AddressProvider testAddressProvider = createAddressProvider(config);
            JetHazelcastClientInstanceImpl client =
                    new JetHazelcastClientInstanceImpl(config, clientConnectionManagerFactory, testAddressProvider);
            client.start();
            clients.add(client);
            OutOfMemoryErrorDispatcher.registerClient(client);
            proxy = new JetHazelcastClientProxy(client);
        } finally {
            Thread.currentThread().setContextClassLoader(tccl);
        }
        return proxy;
    }

    private AddressProvider createAddressProvider(ClientConfig config) {

        List<String> userConfiguredAddresses = config.getNetworkConfig().getAddresses();
        if (!userConfiguredAddresses.contains("localhost")) {
            //Addresses are set explicitly. Dont add more addresses
            return null;
        }

        return new AddressProvider() {
            @Override
            public Collection<InetSocketAddress> loadAddresses() {
                Collection<InetSocketAddress> inetAddresses = new ArrayList<InetSocketAddress>();
                for (Address address : addresses) {
                    Collection<InetSocketAddress> addresses = AddressHelper.getPossibleSocketAddresses(address.getPort(),
                            address.getHost(), 3);
                    inetAddresses.addAll(addresses);
                }

                return inetAddresses;

            }
        };
    }

    @Override
    public void shutdownAll() {
        if (!mockNetwork) {
            HazelcastClient.shutdownAll();
        } else {
            for (HazelcastClientInstanceImpl client : clients) {
                client.shutdown();
            }
        }
        super.shutdownAll();
    }

    @Override
    public void terminateAll() {
        if (!mockNetwork) {
            //For client terminateAll and shutdownAll is same
            HazelcastClient.shutdownAll();
        } else {
            for (HazelcastClientInstanceImpl client : clients) {
                client.getLifecycleService().terminate();
            }
        }
        super.terminateAll();
    }
}

