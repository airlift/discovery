/*
 * Copyright 2010 Proofpoint, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.airlift.discovery.server;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.discovery.client.DiscoveryAnnouncementClient;
import io.airlift.discovery.client.DiscoveryModule;
import io.airlift.discovery.client.ServiceAnnouncement;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.ServiceSelector;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.json.JsonModule;
import io.airlift.node.NodeInfo;
import io.airlift.node.testing.TestingNodeModule;
import org.iq80.leveldb.util.FileUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.weakref.jmx.guice.MBeanModule;
import org.weakref.jmx.testing.TestingMBeanServer;

import javax.management.MBeanServer;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

import java.io.File;
import java.net.URI;
import java.util.List;
import java.util.Map;

import static io.airlift.discovery.client.DiscoveryBinder.discoveryBinder;
import static io.airlift.discovery.client.ServiceTypes.serviceType;
import static io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StatusResponseHandler.StatusResponse;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.json.JsonCodec.mapJsonCodec;
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
                .put("static.db.location", tempDir.getAbsolutePath())
                .put("discovery.store-cache-ttl", "0s")
                .build();

        Bootstrap bootstrap = new Bootstrap(
                new MBeanModule(),
                new TestingNodeModule("testing"),
                new TestingHttpServerModule(),
                new JsonModule(),
                new JaxrsModule(true),
                new DiscoveryServerModule(),
                new DiscoveryModule(),
                new Module()
                {
                    public void configure(Binder binder)
                    {
                        // TODO: use testing mbean server
                        binder.bind(MBeanServer.class).toInstance(new TestingMBeanServer());
                    }
                });

        Injector serverInjector = bootstrap
                .strictConfig()
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(serverProperties)
                .initialize();

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
                .put("discovery.uri", server.getBaseUrl().toString())
                .build();

        Bootstrap bootstrap = new Bootstrap(
                new TestingNodeModule("testing", "red"),
                new JsonModule(),
                new DiscoveryModule());

        Injector announcerInjector = bootstrap
                .strictConfig()
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(announcerProperties)
                .initialize();

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

        HttpClient client = new JettyHttpClient();

        Request request = preparePost()
                .setUri(uriFor("/v1/announcement/static"))
                .setHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
                .setBodyGenerator(jsonBodyGenerator(jsonCodec(Object.class), announcement))
                .build();
        JsonResponse<Map<String, Object>> createResponse = client.execute(request, createFullJsonResponseHandler(mapJsonCodec(String.class, Object.class)));

        assertEquals(createResponse.getStatusCode(), Status.CREATED.getStatusCode());
        String id = createResponse.getValue().get("id").toString();

        List<ServiceDescriptor> services = selectorFor("apple", "red").selectAllServices();
        assertEquals(services.size(), 1);

        ServiceDescriptor service = services.get(0);
        assertEquals(service.getId().toString(), id);
        assertNull(service.getNodeId());
        assertEquals(service.getLocation(), announcement.get("location"));
        assertEquals(service.getPool(), announcement.get("pool"));
        assertEquals(service.getProperties(), announcement.get("properties"));

        // remove announcement
        request = prepareDelete().setUri(uriFor("/v1/announcement/static/" + id)).build();
        StatusResponse deleteResponse = client.execute(request, createStatusResponseHandler());

        assertEquals(deleteResponse.getStatusCode(), Status.NO_CONTENT.getStatusCode());

        // ensure announcement is gone
        assertTrue(selectorFor("apple", "red").selectAllServices().isEmpty());
    }

    private ServiceSelector selectorFor(String type, String pool)
            throws Exception
    {
        Map<String, String> clientProperties = ImmutableMap.<String, String>builder()
            .put("discovery.uri", server.getBaseUrl().toString())
            .put("discovery.apple.pool", "red")
            .build();

        Bootstrap bootstrap = new Bootstrap(
                new TestingNodeModule("testing"),
                new JsonModule(),
                new DiscoveryModule(),
                new Module() {
                    @Override
                    public void configure(Binder binder)
                    {
                        discoveryBinder(binder).bindSelector("apple");
                    }
                });

        Injector clientInjector = bootstrap
                .strictConfig()
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(clientProperties)
                .initialize();

        return clientInjector.getInstance(Key.get(ServiceSelector.class, serviceType("apple")));
    }

    private URI uriFor(String path)
    {
        return server.getBaseUrl().resolve(path);
    }
}
