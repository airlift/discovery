package com.proofpoint.discovery;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.proofpoint.cassandra.testing.CassandraServerSetup;
import com.proofpoint.cassandra.testing.TestingCassandraModule;
import com.proofpoint.configuration.ConfigurationFactory;
import com.proofpoint.configuration.ConfigurationModule;
import com.proofpoint.discovery.client.DiscoveryBinder;
import com.proofpoint.discovery.client.DiscoveryClient;
import com.proofpoint.discovery.client.ServiceAnnouncement;
import com.proofpoint.discovery.client.ServiceDescriptor;
import com.proofpoint.discovery.client.ServiceSelector;
import com.proofpoint.discovery.client.ServiceSelectorConfig;
import com.proofpoint.discovery.client.ServiceTypes;
import com.proofpoint.discovery.client.testing.SimpleServiceSelector;
import com.proofpoint.json.JsonModule;
import com.proofpoint.http.server.testing.TestingHttpServer;
import com.proofpoint.http.server.testing.TestingHttpServerModule;
import com.proofpoint.jaxrs.JaxrsModule;
import com.proofpoint.node.NodeInfo;
import com.proofpoint.node.NodeModule;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestDiscoveryServer
{
    private TestingHttpServer server;

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

    @BeforeMethod
    public void setup()
            throws Exception
    {
        // start server
        Map<String, String> serverProperties = ImmutableMap.<String, String>builder()
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

        // TODO: wrap this in a testing bootstrap that handles PostConstruct & PreDestroy
        CassandraDynamicStore store = serverInjector.getInstance(CassandraDynamicStore.class);
        store.initialize();

        server = serverInjector.getInstance(TestingHttpServer.class);
        server.start();
    }

    @AfterMethod
    public void teardown()
            throws Exception
    {
        server.stop();
    }

    @Test
    public void testDynamicAnnouncement()
            throws Exception
    {
        // publish announcement
        Map<String, String> announcerProperties = ImmutableMap.<String, String>builder()
            .put("node.environment", "testing")
            .put("node.pool", "red")
            .put("discovery.uri", server.getBaseUrl().toString())
            .build();

        Injector announcerInjector = Guice.createInjector(
                new NodeModule(),
                new JsonModule(),
                new ConfigurationModule(new ConfigurationFactory(announcerProperties)),
                new com.proofpoint.discovery.client.DiscoveryModule(),
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
                .addProperties(ImmutableMap.of("key", "value"))
                .build();

        DiscoveryClient client = announcerInjector.getInstance(DiscoveryClient.class);
        client.announce(ImmutableSet.of(announcement));

        NodeInfo announcerNodeInfo = announcerInjector.getInstance(NodeInfo.class);

        // client
        Map<String, String> clientProperties = ImmutableMap.<String, String>builder()
            .put("node.environment", "testing")
            .put("discovery.uri", server.getBaseUrl().toString())
            .put("discovery.apple.pool", "red")
            .build();

        Injector clientInjector = Guice.createInjector(
                new NodeModule(),
                new JsonModule(),
                new ConfigurationModule(new ConfigurationFactory(clientProperties)),
                new com.proofpoint.discovery.client.DiscoveryModule(),
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
        assertEquals(service.getNodeId(), announcerNodeInfo.getNodeId());
        assertEquals(service.getLocation(), announcerNodeInfo.getLocation());
        assertEquals(service.getPool(), announcerNodeInfo.getPool());
        assertEquals(service.getProperties(), announcement.getProperties());


        // ensure that service is no longer visible
        client.unannounce();

        ServiceSelector freshSelector = new SimpleServiceSelector("apple", new ServiceSelectorConfig().setPool("red"), client);
        assertTrue(freshSelector.selectAllServices().isEmpty());
    }
}
