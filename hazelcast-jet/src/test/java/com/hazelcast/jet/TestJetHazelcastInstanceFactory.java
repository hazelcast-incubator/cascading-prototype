package com.hazelcast.jet;


import java.util.List;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;


import com.hazelcast.nio.Address;


import com.hazelcast.config.Config;

import com.hazelcast.core.Hazelcast;

import java.net.UnknownHostException;

import com.hazelcast.instance.NodeContext;
import com.hazelcast.test.TestEnvironment;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.config.XmlConfigBuilder;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.test.mocknetwork.TestNodeRegistry;
import com.hazelcast.jet.api.hazelcast.JetHazelcastInstance;
import com.hazelcast.jet.impl.hazelcast.JetHazelcastInstanceFactory;

import static com.hazelcast.instance.TestUtil.getNode;
import static com.hazelcast.instance.TestUtil.terminateInstance;
import static com.hazelcast.util.Preconditions.checkNotNull;

public class TestJetHazelcastInstanceFactory {
    private static final AtomicInteger PORTS = new AtomicInteger(5000);

    private final boolean mockNetwork = TestEnvironment.isMockNetwork();
    private final AtomicInteger nodeIndex = new AtomicInteger();

    protected TestNodeRegistry registry;
    protected final CopyOnWriteArrayList<Address> addresses = new CopyOnWriteArrayList<Address>();
    private final int count;

    private final JetHazelcastInstanceFactory factory = new JetHazelcastInstanceFactory();

    public TestJetHazelcastInstanceFactory(int count) {
        this.count = count;
        if (mockNetwork) {
            List<Address> addresses = createAddresses(PORTS, count);
            initFactory(addresses);
        }
    }

    public TestJetHazelcastInstanceFactory() {
        this.count = 0;
        this.registry = new TestNodeRegistry(addresses);
    }

    public TestJetHazelcastInstanceFactory(int initialPort, String... addresses) {
        this.count = addresses.length;
        if (mockNetwork) {
            List<Address> addressList = createAddresses(initialPort, PORTS, addresses);
            initFactory(addressList);
        }
    }

    public TestJetHazelcastInstanceFactory(String... addresses) {
        this.count = addresses.length;
        if (mockNetwork) {
            List<Address> addressesList = createAddresses(-1, PORTS, addresses);
            initFactory(addressesList);
        }
    }

    public TestJetHazelcastInstanceFactory(Collection<Address> addresses) {
        this.count = addresses.size();
        if (mockNetwork) {
            initFactory(addresses);
        }
    }

    private void initFactory(Collection<Address> addresses) {
        this.addresses.addAll(addresses);
        this.registry = new TestNodeRegistry(this.addresses);
    }

    /**
     * Delegates to {@link #newHazelcastInstance(Config) <code>newHazelcastInstance(null)</code>}.
     */
    public JetHazelcastInstance newHazelcastInstance() {
        return newHazelcastInstance((Config) null);
    }

    /**
     * Creates a new test Hazelcast instance.
     *
     * @param config the config to use; use <code>null</code> to get the default config.
     */
    public JetHazelcastInstance newHazelcastInstance(Config config) {
        String instanceName = config != null ? config.getInstanceName() : null;
        if (mockNetwork) {
            init(config);
            NodeContext nodeContext = registry.createNodeContext(pickAddress());
            return factory.newHazelcastInstance(config, instanceName, nodeContext);
        }
        return factory.newHazelcastInstance(config);
    }

    /**
     * Creates a new test Hazelcast instance.
     *
     * @param address the address to use as Member's address instead of picking the next address
     */
    public HazelcastInstance newHazelcastInstance(Address address) {
        return newHazelcastInstance(address, null);
    }

    /**
     * Creates a new test Hazelcast instance.
     *
     * @param address the address to use as Member's address instead of picking the next address
     * @param config  the config to use; use <code>null</code> to get the default config.
     */
    public HazelcastInstance newHazelcastInstance(Address address, Config config) {
        final String instanceName = config != null ? config.getInstanceName() : null;
        if (mockNetwork) {
            init(config);
            NodeContext nodeContext = registry.createNodeContext(address);
            return HazelcastInstanceFactory.newHazelcastInstance(config, instanceName, nodeContext);
        }
        throw new UnsupportedOperationException("Explicit address is only available for mock network setup!");
    }

    private Address pickAddress() {
        int id = nodeIndex.getAndIncrement();
        if (addresses.size() > id) {
            return addresses.get(id);
        }
        Address address = createAddress("127.0.0.1", PORTS.incrementAndGet());
        addresses.add(address);
        return address;
    }

    public HazelcastInstance[] newInstances() {
        return newInstances(new Config());
    }

    public HazelcastInstance[] newInstances(Config config, int nodeCount) {
        HazelcastInstance[] instances = new HazelcastInstance[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            instances[i] = newHazelcastInstance(config);
        }
        return instances;
    }

    public HazelcastInstance[] newInstances(Config config) {
        return newInstances(config, count);
    }

    public Collection<HazelcastInstance> getAllHazelcastInstances() {
        if (mockNetwork) {
            return registry.getAllHazelcastInstances();
        }
        return Hazelcast.getAllHazelcastInstances();
    }

    /**
     * Terminates supplied instance by releasing internal resources.
     *
     * @param instance the instance.
     */
    public void terminate(HazelcastInstance instance) {
        Address address = getNode(instance).address;
        terminateInstance(instance);
        registry.removeInstance(address);
    }

    public void shutdownAll() {
        if (mockNetwork) {
            registry.shutdown();
        } else {
            Hazelcast.shutdownAll();
        }
    }

    public void terminateAll() {
        if (mockNetwork) {
            registry.terminate();
        } else {
            HazelcastInstanceFactory.terminateAll();
        }
    }

    private static List<Address> createAddresses(AtomicInteger ports, int count) {
        List<Address> addresses = new ArrayList<Address>(count);
        for (int i = 0; i < count; i++) {
            addresses.add(createAddress("127.0.0.1", ports.incrementAndGet()));
        }
        return addresses;
    }

    private static List<Address> createAddresses(int initialPort, AtomicInteger ports, String... addressArray) {
        checkElementsNotNull(addressArray);

        int count = addressArray.length;
        List<Address> addresses = new ArrayList<Address>(count);
        for (String address : addressArray) {
            int port = initialPort == -1 ? ports.incrementAndGet() : initialPort++;
            addresses.add(createAddress(address, port));
        }
        return addresses;
    }

    protected static Address createAddress(String host, int port) {
        try {
            return new Address(host, port);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static <T> void checkElementsNotNull(T[] array) {
        checkNotNull(array, "Array should not be null");
        for (Object element : array) {
            checkNotNull(element, "Array element should not be null");
        }
    }

    private static Config init(Config config) {
        if (config == null) {
            config = new XmlConfigBuilder().build();
        }
        config.setProperty(GroupProperty.WAIT_SECONDS_BEFORE_JOIN, "0");
        config.setProperty(GroupProperty.GRACEFUL_SHUTDOWN_MAX_WAIT, "120");
        config.setProperty(GroupProperty.PARTITION_BACKUP_SYNC_INTERVAL, "1");
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        return config;
    }

    public Collection<Address> getKnownAddresses() {
        return Collections.unmodifiableCollection(addresses);
    }

    @Override
    public String toString() {
        return "TestJetHazelcastInstanceFactory{addresses=" + addresses + '}';
    }

    public TestNodeRegistry getRegistry() {
        return registry;
    }
}
