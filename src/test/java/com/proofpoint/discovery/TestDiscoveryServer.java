package com.proofpoint.discovery;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.proofpoint.configuration.ConfigurationFactory;
import com.proofpoint.configuration.ConfigurationModule;
import com.proofpoint.experimental.discovery.client.DiscoveryBinder;
import com.proofpoint.experimental.discovery.client.DiscoveryClient;
import com.proofpoint.experimental.discovery.client.ServiceAnnouncement;
import com.proofpoint.experimental.discovery.client.ServiceDescriptor;
import com.proofpoint.experimental.discovery.client.ServiceSelector;
import com.proofpoint.experimental.discovery.client.ServiceTypes;
import com.proofpoint.experimental.json.JsonModule;
import com.proofpoint.http.server.testing.TestingHttpServer;
import com.proofpoint.http.server.testing.TestingHttpServerModule;
import com.proofpoint.jaxrs.JaxrsModule;
import com.proofpoint.node.NodeInfo;
import com.proofpoint.node.NodeModule;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestDiscoveryServer
{
    @BeforeSuite
    public void setupCassandra()
            throws IOException, TTransportException, ConfigurationException, InterruptedException
    {
        CassandraServerSetup.tryInitialize();
    }

    @AfterSuite
    public void teardownCassandra()
            throws IOException
    {
        CassandraServerSetup.tryShutdown();
    }

    @Test
    public void test()
            throws Exception
    {
        // start server
        Map<String, String> serverProperties = new ImmutableMap.Builder<String, String>()
                    .put("node.environment", "testing")
                    .build();

        Injector serverInjector = Guice.createInjector(
                new NodeModule(),
                new TestingHttpServerModule(),
                new JsonModule(),
                new JaxrsModule(),
                new DiscoveryModule(),
                new TestingCassandraModule(),
                new ConfigurationModule(new ConfigurationFactory(serverProperties)));

        TestingHttpServer server = serverInjector.getInstance(TestingHttpServer.class);
        server.start();

        // publish announcement
        Map<String, String> announcerProperties = new ImmutableMap.Builder<String, String>()
            .put("node.environment", "testing")
            .put("discovery.uri", server.getBaseUrl().toString())
            .build();

        Injector announcerInjector = Guice.createInjector(
                new NodeModule(),
                new JsonModule(),
                new ConfigurationModule(new ConfigurationFactory(announcerProperties)),
                new com.proofpoint.experimental.discovery.client.DiscoveryModule(),
                new Module()
                {
                    @Override
                    public void configure(Binder binder)
                    {
                        DiscoveryBinder.discoveryBinder(binder).bindSelector("apple");
                    }
                }
        );

        ServiceAnnouncement announcement = ServiceAnnouncement.serviceAnnouncement("apple")
                .setPool("red")
                .addProperties(ImmutableMap.of("key", "value"))
                .build();

        DiscoveryClient client = announcerInjector.getInstance(DiscoveryClient.class);
        client.announce(ImmutableSet.of(announcement));

        NodeInfo announcerInfo = announcerInjector.getInstance(NodeInfo.class);

        // client
        Map<String, String> clientProperties = new ImmutableMap.Builder<String, String>()
            .put("node.environment", "testing")
            .put("discovery.uri", server.getBaseUrl().toString())
            .put("discovery.apple.pool", "red")
            .build();

        Injector clientInjector = Guice.createInjector(
                new NodeModule(),
                new JsonModule(),
                new ConfigurationModule(new ConfigurationFactory(clientProperties)),
                new com.proofpoint.experimental.discovery.client.DiscoveryModule(),
                new Module()
                {
                    @Override
                    public void configure(Binder binder)
                    {
                        DiscoveryBinder.discoveryBinder(binder).bindSelector("apple");
                    }
                }
        );


        ServiceSelector selector = clientInjector.getInstance(Key.get(ServiceSelector.class, ServiceTypes.serviceType("apple")));

        List<ServiceDescriptor> services = selector.selectAllServices();
        assertEquals(services.size(), 1);

        ServiceDescriptor service = services.get(0);
        assertNotNull(service.getId());
        assertEquals(service.getNodeId(), announcerInfo.getNodeId());
        assertEquals(service.getLocation(), announcerInfo.getNodeId()); // TODO: fix once client supports location
        assertEquals(service.getPool(), announcement.getPool());
        assertEquals(service.getProperties(), announcement.getProperties());
    }
}
