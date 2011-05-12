package com.proofpoint.discovery;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.proofpoint.jaxrs.testing.MockUriInfo;
import com.proofpoint.node.NodeInfo;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.ws.rs.core.Response;
import java.net.URI;
import java.util.UUID;

import static com.google.common.collect.Iterables.transform;
import static com.proofpoint.discovery.DynamicServiceAnnouncement.toServiceWith;
import static com.proofpoint.testing.Assertions.assertEqualsIgnoreCase;
import static com.proofpoint.testing.Assertions.assertEqualsIgnoreOrder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestDynamicAnnouncementResource
{
    private InMemoryDynamicStore store;
    private DynamicAnnouncementResource resource;

    @BeforeMethod
    public void setup()
    {
        store = new InMemoryDynamicStore(new DiscoveryConfig(), new RealTimeProvider());
        resource = new DynamicAnnouncementResource(store, new NodeInfo("testing"));
    }

    @Test
    public void testPutNew()
    {
        DynamicAnnouncement announcement = new DynamicAnnouncement("testing", "/a/b/c", ImmutableSet.of(
                new DynamicServiceAnnouncement(UUID.randomUUID(), "storage", "alpha", ImmutableMap.of("http", "http://localhost:1111")))
        );

        UUID nodeId = UUID.randomUUID();
        Response response = resource.put(nodeId, new MockUriInfo(URI.create("http://localhost:8080/v1/announcement/" + nodeId.toString())), announcement);

        assertNotNull(response);
        assertEquals(response.getStatus(), Response.Status.CREATED.getStatusCode());

        assertEqualsIgnoreOrder(store.getAll(), transform(announcement.getServiceAnnouncements(), toServiceWith(nodeId, announcement.getLocation())));
    }

    @Test
    public void testReplace()
    {
        UUID nodeId = UUID.randomUUID();
        DynamicAnnouncement previous = new DynamicAnnouncement("testing", "/a/b/c", ImmutableSet.of(
                new DynamicServiceAnnouncement(UUID.randomUUID(), "storage", "alpha", ImmutableMap.of("key", "existing"))
        ));

        store.put(nodeId, previous);

        DynamicAnnouncement announcement = new DynamicAnnouncement("testing", "/a/b/c", ImmutableSet.of(
                new DynamicServiceAnnouncement(UUID.randomUUID(), "storage", "alpha", ImmutableMap.of("key", "new")))
        );

        Response response = resource.put(nodeId, new MockUriInfo(URI.create("http://localhost:8080/v1/announcement/" + nodeId.toString())), announcement);

        assertNotNull(response);
        assertEquals(response.getStatus(), Response.Status.NO_CONTENT.getStatusCode());

        assertEqualsIgnoreOrder(store.getAll(), transform(announcement.getServiceAnnouncements(), toServiceWith(nodeId, announcement.getLocation())));
    }

    @Test
    public void testEnvironmentConflict()
    {
        DynamicServiceAnnouncement serviceAnnouncement = new DynamicServiceAnnouncement(UUID.randomUUID(), "storage", "alpha", ImmutableMap.of("http", "http://localhost:1111"));
        DynamicAnnouncement announcement = new DynamicAnnouncement("production", "/a/b/c", ImmutableSet.of(serviceAnnouncement));

        UUID nodeId = UUID.randomUUID();
        Response response = resource.put(nodeId, new MockUriInfo(URI.create("http://localhost:8080/v1/announcement/" + nodeId.toString())), announcement);

        assertNotNull(response);
        assertEquals(response.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());

        assertTrue(store.getAll().isEmpty());
    }

    @Test
    public void testDeleteExisting()
    {
        UUID blueNodeId = UUID.randomUUID();
        DynamicAnnouncement blue = new DynamicAnnouncement("testing", "/a/b/c", ImmutableSet.of(
                new DynamicServiceAnnouncement(UUID.randomUUID(), "storage", "alpha", ImmutableMap.of("key", "valueBlue"))
        ));

        UUID redNodeId = UUID.randomUUID();
        DynamicAnnouncement red = new DynamicAnnouncement("testing", "/a/b/c", ImmutableSet.of(
                new DynamicServiceAnnouncement(UUID.randomUUID(), "storage", "alpha", ImmutableMap.of("key", "valueBlue"))
        ));

        store.put(redNodeId, red);
        store.put(blueNodeId, blue);

        Response response = resource.delete(blueNodeId);

        assertNotNull(response);
        assertEquals(response.getStatus(), Response.Status.NO_CONTENT.getStatusCode());

        assertEqualsIgnoreOrder(store.getAll(), transform(red.getServiceAnnouncements(), toServiceWith(redNodeId, red.getLocation())));
    }

    @Test
    public void testDeleteMissing()
    {
        Response response = resource.delete(UUID.randomUUID());

        assertNotNull(response);
        assertEquals(response.getStatus(), Response.Status.NOT_FOUND.getStatusCode());

        assertTrue(store.getAll().isEmpty());
    }

    @Test
    public void testMakesUpLocation()
    {
        DynamicServiceAnnouncement serviceAnnouncement = new DynamicServiceAnnouncement(UUID.randomUUID(), "storage", "alpha", ImmutableMap.of("http", "http://localhost:1111"));
        DynamicAnnouncement announcement = new DynamicAnnouncement("testing", null, ImmutableSet.of(serviceAnnouncement));

        UUID nodeId = UUID.randomUUID();
        Response response = resource.put(nodeId, new MockUriInfo(URI.create("http://localhost:8080/v1/announcement/" + nodeId.toString())), announcement);

        assertNotNull(response);
        assertEquals(response.getStatus(), Response.Status.CREATED.getStatusCode());

        assertEquals(store.getAll().size(), 1);
        Service service = store.getAll().iterator().next();
        assertEquals(service.getId(), service.getId());
        assertNotNull(service.getLocation());
    }
}
