package com.proofpoint.discovery;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import com.proofpoint.configuration.ConfigurationFactory;
import com.proofpoint.configuration.ConfigurationModule;
import com.proofpoint.discovery.client.DiscoveryAnnouncementClient;
import com.proofpoint.discovery.client.DiscoveryLookupClient;
import com.proofpoint.discovery.client.DiscoveryModule;
import com.proofpoint.discovery.client.ServiceAnnouncement;
import com.proofpoint.discovery.client.ServiceDescriptor;
import com.proofpoint.discovery.client.ServiceSelector;
import com.proofpoint.discovery.client.ServiceSelectorConfig;
import com.proofpoint.discovery.client.testing.SimpleServiceSelector;
import com.proofpoint.discovery.store.ReplicatedStoreModule;
import com.proofpoint.http.server.testing.TestingHttpServer;
import com.proofpoint.http.server.testing.TestingHttpServerModule;
import com.proofpoint.jaxrs.JaxrsModule;
import com.proofpoint.json.JsonCodec;
import com.proofpoint.json.JsonModule;
import com.proofpoint.node.NodeInfo;
import com.proofpoint.node.NodeModule;
import org.iq80.leveldb.util.FileUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.weakref.jmx.guice.MBeanModule;

import javax.management.MBeanServer;
import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Map;

import static com.proofpoint.json.JsonCodec.mapJsonCodec;
import static javax.ws.rs.core.Response.Status;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestDiscoveryServer
{
    private TestingHttpServer server;
    private File tempDir;

    @BeforeMethod
    public void setup()
            throws Exception
    {
        tempDir = Files.createTempDir();

        // start server
        Map<String, String> serverProperties = ImmutableMap.<String, String>builder()
                    .put("node.environment", "testing")
                    .put("static.db.location", tempDir.getAbsolutePath())
                    .build();

        Injector serverInjector = Guice.createInjector(
                new MBeanModule(),
                new NodeModule(),
                new TestingHttpServerModule(),
                new JsonModule(),
                new JaxrsModule(),
                new DiscoveryServerModule(),
                new DiscoveryModule(),
                new ConfigurationModule(new ConfigurationFactory(serverProperties)),
                new Module() {
                    public void configure(Binder binder)
                    {
                        // TODO: use testing mbean server
                        binder.bind(MBeanServer.class).toInstance(ManagementFactory.getPlatformMBeanServer());
                    }
                });

        server = serverInjector.getInstance(TestingHttpServer.class);
        server.start();
    }

    @AfterMethod
    public void teardown()
            throws Exception
    {
        server.stop();
        FileUtils.deleteRecursively(tempDir);
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
                new com.proofpoint.discovery.client.DiscoveryModule()
        );

        ServiceAnnouncement announcement = ServiceAnnouncement.serviceAnnouncement("apple")
                .addProperties(ImmutableMap.of("key", "value"))
                .build();

        DiscoveryAnnouncementClient client = announcerInjector.getInstance(DiscoveryAnnouncementClient.class);
        client.announce(ImmutableSet.of(announcement)).get();

        NodeInfo announcerNodeInfo = announcerInjector.getInstance(NodeInfo.class);

        List<ServiceDescriptor> services = selectorFor("apple", "red").selectAllServices();
        assertEquals(services.size(), 1);

        ServiceDescriptor service = services.get(0);
        assertNotNull(service.getId());
        assertEquals(service.getNodeId(), announcerNodeInfo.getNodeId());
        assertEquals(service.getLocation(), announcerNodeInfo.getLocation());
        assertEquals(service.getPool(), announcerNodeInfo.getPool());
        assertEquals(service.getProperties(), announcement.getProperties());


        // ensure that service is no longer visible
        client.unannounce().get();

        assertTrue(selectorFor("apple", "red").selectAllServices().isEmpty());
    }


    @Test
    public void testStaticAnnouncement()
            throws Exception
    {
        // create static announcement
        Map<String, Object> announcement = ImmutableMap.<String, Object>builder()
                .put("environment", "testing")
                .put("type", "apple")
                .put("pool", "red")
                .put("location", "/a/b/c")
                .put("properties", ImmutableMap.of("http", "http://host"))
                .build();

        AsyncHttpClient httpClient = new AsyncHttpClient();
        Response response = httpClient.preparePost(server.getBaseUrl().resolve("/v1/announcement/static").toString())
                .addHeader("Content-Type", "application/json")
                .setBody(JsonCodec.jsonCodec(Object.class).toJson(announcement))
                .execute()
                .get();

        assertEquals(response.getStatusCode(), Status.CREATED.getStatusCode());
        String id = mapJsonCodec(String.class, Object.class)
                .fromJson(response.getResponseBody())
                .get("id")
                .toString();

        List<ServiceDescriptor> services = selectorFor("apple", "red").selectAllServices();
        assertEquals(services.size(), 1);

        ServiceDescriptor service = services.get(0);
        assertEquals(service.getId().toString(), id);
        assertNull(service.getNodeId());
        assertEquals(service.getLocation(), announcement.get("location"));
        assertEquals(service.getPool(), announcement.get("pool"));
        assertEquals(service.getProperties(), announcement.get("properties"));

        // remove announcement
        response = httpClient.prepareDelete(server.getBaseUrl().resolve("/v1/announcement/static/" + id).toString())
                .execute()
                .get();

        assertEquals(response.getStatusCode(), Status.NO_CONTENT.getStatusCode());

        // ensure announcement is gone
        assertTrue(selectorFor("apple", "red").selectAllServices().isEmpty());
    }

    private ServiceSelector selectorFor(String type, String pool)
    {
        Map<String, String> clientProperties = ImmutableMap.<String, String>builder()
            .put("node.environment", "testing")
            .put("discovery.uri", server.getBaseUrl().toString())
            .put("discovery.apple.pool", "red")
            .build();

        Injector clientInjector = Guice.createInjector(
                new NodeModule(),
                new JsonModule(),
                new ConfigurationModule(new ConfigurationFactory(clientProperties)),
                new com.proofpoint.discovery.client.DiscoveryModule()
        );

        DiscoveryLookupClient client = clientInjector.getInstance(DiscoveryLookupClient.class);
        return new SimpleServiceSelector(type, new ServiceSelectorConfig().setPool(pool), client);
    }
}
