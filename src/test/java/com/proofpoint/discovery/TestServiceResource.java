package com.proofpoint.discovery;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.proofpoint.node.NodeInfo;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.UUID;

import static com.google.common.collect.Collections2.transform;
import static com.google.common.collect.ImmutableSet.*;
import static com.proofpoint.discovery.DynamicServiceAnnouncement.toServiceWith;
import static org.testng.Assert.assertEquals;

public class TestServiceResource
{
    private InMemoryDynamicStore dynamicStore;
    private InMemoryStaticStore staticStore;
    private ServiceResource resource;

    @BeforeMethod
    protected void setUp()
    {
        dynamicStore = new InMemoryDynamicStore(new DiscoveryConfig(), new TestingTimeProvider());
        staticStore = new InMemoryStaticStore();
        resource = new ServiceResource(dynamicStore, staticStore, new NodeInfo("testing"));
    }

    @Test
    public void testGetByType()
    {
        UUID redNodeId = UUID.randomUUID();
        DynamicServiceAnnouncement redStorage = new DynamicServiceAnnouncement(UUID.randomUUID(), "storage", "alpha", ImmutableMap.of("key", "1"));
        DynamicServiceAnnouncement redWeb = new DynamicServiceAnnouncement(UUID.randomUUID(), "web", "alpha", ImmutableMap.of("key", "2"));
        DynamicAnnouncement red = new DynamicAnnouncement("testing", "/a/b/c", of(redStorage, redWeb));

        UUID greenNodeId = UUID.randomUUID();
        DynamicServiceAnnouncement greenStorage = new DynamicServiceAnnouncement(UUID.randomUUID(), "storage", "alpha", ImmutableMap.of("key", "3"));
        DynamicAnnouncement green = new DynamicAnnouncement("testing", "/x/y/z", of(greenStorage));

        UUID blueNodeId = UUID.randomUUID();
        DynamicServiceAnnouncement blueStorage = new DynamicServiceAnnouncement(UUID.randomUUID(), "storage", "beta", ImmutableMap.of("key", "4"));
        DynamicAnnouncement blue = new DynamicAnnouncement("testing", "/a/b/c", of(blueStorage));

        dynamicStore.put(redNodeId, red);
        dynamicStore.put(greenNodeId, green);
        dynamicStore.put(blueNodeId, blue);

        assertEquals(resource.getServices("storage"), new Services("testing", of(
                toServiceWith(redNodeId, "/a/b/c").apply(redStorage),
                toServiceWith(greenNodeId, "/x/y/z").apply(greenStorage),
                toServiceWith(blueNodeId, "/a/b/c").apply(blueStorage))));

        assertEquals(resource.getServices("web"), new Services("testing", ImmutableSet.of(
                toServiceWith(redNodeId, "/a/b/c").apply(redWeb))));

        assertEquals(resource.getServices("unknown"), new Services("testing", Collections.<Service>emptySet()));
    }

    @Test
    public void testGetByTypeAndPool()
    {
        UUID redNodeId = UUID.randomUUID();
        DynamicServiceAnnouncement redStorage = new DynamicServiceAnnouncement(UUID.randomUUID(), "storage", "alpha", ImmutableMap.of("key", "1"));
        DynamicServiceAnnouncement redWeb = new DynamicServiceAnnouncement(UUID.randomUUID(), "web", "alpha", ImmutableMap.of("key", "2"));
        DynamicAnnouncement red = new DynamicAnnouncement("testing", "/a/b/c", of(redStorage, redWeb));

        UUID greenNodeId = UUID.randomUUID();
        DynamicServiceAnnouncement greenStorage = new DynamicServiceAnnouncement(UUID.randomUUID(), "storage", "alpha", ImmutableMap.of("key", "3"));
        DynamicAnnouncement green = new DynamicAnnouncement("testing", "/x/y/z", of(greenStorage));

        UUID blueNodeId = UUID.randomUUID();
        DynamicServiceAnnouncement blueStorage = new DynamicServiceAnnouncement(UUID.randomUUID(), "storage", "beta", ImmutableMap.of("key", "4"));
        DynamicAnnouncement blue = new DynamicAnnouncement("testing", "/a/b/c", of(blueStorage));

        dynamicStore.put(redNodeId, red);
        dynamicStore.put(greenNodeId, green);
        dynamicStore.put(blueNodeId, blue);

        assertEquals(resource.getServices("storage", "alpha"), new Services("testing", ImmutableSet.of(
                toServiceWith(redNodeId, "/a/b/c").apply(redStorage),
                toServiceWith(greenNodeId, "/x/y/z").apply(greenStorage))));

        assertEquals(resource.getServices("storage", "beta"), new Services("testing", ImmutableSet.of(toServiceWith(blueNodeId, "/a/b/c").apply(blueStorage))));

        assertEquals(resource.getServices("storage", "unknown"), new Services("testing", Collections.<Service>emptySet()));
    }
}
