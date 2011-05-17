package com.proofpoint.discovery;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.proofpoint.node.NodeInfo;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;

import static com.google.common.collect.ImmutableSet.of;
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
        Id<Node> redNodeId = Id.random();
        DynamicServiceAnnouncement redStorage = new DynamicServiceAnnouncement(Id.<Service>random() , "storage", ImmutableMap.of("key", "1"));
        DynamicServiceAnnouncement redWeb = new DynamicServiceAnnouncement(Id.<Service>random(), "web", ImmutableMap.of("key", "2"));
        DynamicAnnouncement red = new DynamicAnnouncement("testing", "alpha", "/a/b/c", of(redStorage, redWeb));

        Id<Node> greenNodeId = Id.random();
        DynamicServiceAnnouncement greenStorage = new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("key", "3"));
        DynamicAnnouncement green = new DynamicAnnouncement("testing", "alpha", "/x/y/z", of(greenStorage));

        Id<Node> blueNodeId = Id.random();
        DynamicServiceAnnouncement blueStorage = new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("key", "4"));
        DynamicAnnouncement blue = new DynamicAnnouncement("testing", "beta", "/a/b/c", of(blueStorage));

        dynamicStore.put(redNodeId, red);
        dynamicStore.put(greenNodeId, green);
        dynamicStore.put(blueNodeId, blue);

        assertEquals(resource.getServices("storage"), new Services("testing", of(
                toServiceWith(redNodeId, red.getLocation(), red.getPool()).apply(redStorage),
                toServiceWith(greenNodeId, green.getLocation(), green.getPool()).apply(greenStorage),
                toServiceWith(blueNodeId, blue.getLocation(), blue.getPool()).apply(blueStorage))));

        assertEquals(resource.getServices("web"), new Services("testing", ImmutableSet.of(
                toServiceWith(redNodeId, red.getLocation(), red.getPool()).apply(redWeb))));

        assertEquals(resource.getServices("unknown"), new Services("testing", Collections.<Service>emptySet()));
    }

    @Test
    public void testGetByTypeAndPool()
    {
        Id<Node> redNodeId = Id.random();
        DynamicServiceAnnouncement redStorage = new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("key", "1"));
        DynamicServiceAnnouncement redWeb = new DynamicServiceAnnouncement(Id.<Service>random(), "web", ImmutableMap.of("key", "2"));
        DynamicAnnouncement red = new DynamicAnnouncement("testing", "alpha", "/a/b/c", of(redStorage, redWeb));

        Id<Node> greenNodeId = Id.random();
        DynamicServiceAnnouncement greenStorage = new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("key", "3"));
        DynamicAnnouncement green = new DynamicAnnouncement("testing", "alpha", "/x/y/z", of(greenStorage));

        Id<Node> blueNodeId = Id.random();
        DynamicServiceAnnouncement blueStorage = new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("key", "4"));
        DynamicAnnouncement blue = new DynamicAnnouncement("testing", "beta", "/a/b/c", of(blueStorage));

        dynamicStore.put(redNodeId, red);
        dynamicStore.put(greenNodeId, green);
        dynamicStore.put(blueNodeId, blue);

        assertEquals(resource.getServices("storage", "alpha"), new Services("testing", ImmutableSet.of(
                toServiceWith(redNodeId, red.getLocation(), red.getPool()).apply(redStorage),
                toServiceWith(greenNodeId, green.getLocation(), green.getPool()).apply(greenStorage))));

        assertEquals(resource.getServices("storage", "beta"), new Services("testing", ImmutableSet.of(toServiceWith(blueNodeId, blue.getLocation(), blue.getPool()).apply(blueStorage))));

        assertEquals(resource.getServices("storage", "unknown"), new Services("testing", Collections.<Service>emptySet()));
    }

    @Test
    public void testGetAll()
    {
        Id<Node> redNodeId = Id.random();
        DynamicServiceAnnouncement redStorage = new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("key", "1"));
        DynamicServiceAnnouncement redWeb = new DynamicServiceAnnouncement(Id.<Service>random(), "web", ImmutableMap.of("key", "2"));
        DynamicAnnouncement red = new DynamicAnnouncement("testing", "alpha", "/a/b/c", of(redStorage, redWeb));

        Id<Node> greenNodeId = Id.random();
        DynamicServiceAnnouncement greenStorage = new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("key", "3"));
        DynamicAnnouncement green = new DynamicAnnouncement("testing", "alpha", "/x/y/z", of(greenStorage));

        Id<Node> blueNodeId = Id.random();
        DynamicServiceAnnouncement blueStorage = new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("key", "4"));
        DynamicAnnouncement blue = new DynamicAnnouncement("testing", "beta", "/a/b/c", of(blueStorage));

        dynamicStore.put(redNodeId, red);
        dynamicStore.put(greenNodeId, green);
        dynamicStore.put(blueNodeId, blue);

        assertEquals(resource.getServices(), new Services("testing", ImmutableSet.of(
                toServiceWith(redNodeId, red.getLocation(), red.getPool()).apply(redStorage),
                toServiceWith(redNodeId, red.getLocation(), red.getPool()).apply(redWeb),
                toServiceWith(greenNodeId, green.getLocation(), green.getPool()).apply(greenStorage),
                toServiceWith(blueNodeId, blue.getLocation(), blue.getPool()).apply(blueStorage))));
    }
}
