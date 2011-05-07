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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestAnnouncementResource
{
    private InMemoryStore store;
    private AnnouncementResource resource;

    @BeforeMethod
    public void setup()
    {
        store = new InMemoryStore(new DiscoveryConfig(), new RealTimeProvider());
        resource = new AnnouncementResource(store, new NodeInfo("testing"));
    }

    @Test
    public void testPutNew()
    {
        ServiceAnnouncement serviceAnnouncement = new ServiceAnnouncement(UUID.randomUUID(), "storage", "alpha", ImmutableMap.of("http", "http://localhost:1111"));
        Announcement announcement = new Announcement("testing", "/a/b/c", ImmutableSet.of(serviceAnnouncement));

        UUID nodeId = UUID.randomUUID();
        Response response = resource.put(nodeId, new MockUriInfo(URI.create("http://localhost:8080/v1/announcement/" + nodeId.toString())), announcement);

        assertNotNull(response);
        assertEquals(response.getStatus(), Response.Status.CREATED.getStatusCode());

        Service expected = Service.copyOf(serviceAnnouncement)
                .setLocation(announcement.getLocation())
                .setNodeId(nodeId)
                .build();

        assertEquals(store.getAll(), ImmutableSet.of(expected));
    }

    @Test
    public void testReplace()
    {
        UUID nodeId = UUID.randomUUID();
        Service existing = new Service(UUID.randomUUID(), nodeId, "storage", "alpha", "/a/b/c", ImmutableMap.of("key", "existing"));

        store.put(nodeId, ImmutableSet.of(existing));


        ServiceAnnouncement serviceAnnouncement = new ServiceAnnouncement(UUID.randomUUID(), "storage", "alpha", ImmutableMap.of("key", "new"));
        Announcement announcement = new Announcement("testing", "/a/b/c", ImmutableSet.of(serviceAnnouncement));

        Response response = resource.put(nodeId, new MockUriInfo(URI.create("http://localhost:8080/v1/announcement/" + nodeId.toString())), announcement);

        assertNotNull(response);
        assertEquals(response.getStatus(), Response.Status.NO_CONTENT.getStatusCode());

        Service expected = Service.copyOf(serviceAnnouncement)
                .setLocation(announcement.getLocation())
                .setNodeId(nodeId)
                .build();

        assertEquals(store.getAll(), ImmutableSet.of(expected));
    }

    @Test
    public void testEnvironmentConflict()
    {
        ServiceAnnouncement serviceAnnouncement = new ServiceAnnouncement(UUID.randomUUID(), "storage", "alpha", ImmutableMap.of("http", "http://localhost:1111"));
        Announcement announcement = new Announcement("production", "/a/b/c", ImmutableSet.of(serviceAnnouncement));

        UUID nodeId = UUID.randomUUID();
        Response response = resource.put(nodeId, new MockUriInfo(URI.create("http://localhost:8080/v1/announcement/" + nodeId.toString())), announcement);

        assertNotNull(response);
        assertEquals(response.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());

        assertTrue(store.getAll().isEmpty());
    }

    @Test
    public void testDeleteExisting()
    {
        Service blue = new Service(UUID.randomUUID(), UUID.randomUUID(), "storage", "alpha", "/a/b/c", ImmutableMap.of("key", "valueBlue"));
        Service red = new Service(UUID.randomUUID(), UUID.randomUUID(), "storage", "alpha", "/a/b/c", ImmutableMap.of("key", "valueRed"));

        store.put(red.getNodeId(), ImmutableSet.of(red));
        store.put(blue.getNodeId(), ImmutableSet.of(blue));

        Response response = resource.delete(blue.getNodeId());

        assertNotNull(response);
        assertEquals(response.getStatus(), Response.Status.NO_CONTENT.getStatusCode());

        assertEquals(store.getAll(), ImmutableSet.of(red));
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
        ServiceAnnouncement serviceAnnouncement = new ServiceAnnouncement(UUID.randomUUID(), "storage", "alpha", ImmutableMap.of("http", "http://localhost:1111"));
        Announcement announcement = new Announcement("testing", null, ImmutableSet.of(serviceAnnouncement));

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
