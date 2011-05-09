package com.proofpoint.discovery;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.proofpoint.node.NodeInfo;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.UUID;

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
        Service red1 = new Service(UUID.randomUUID(), redNodeId, "storage", "alpha", "/a/b/c", ImmutableMap.of("key", "1"));
        Service red2 = new Service(UUID.randomUUID(), redNodeId, "data", "alpha", "/a/b/c", ImmutableMap.of("key", "4"));
        Service green = new Service(UUID.randomUUID(), UUID.randomUUID(), "storage", "alpha", "/a/b/c", ImmutableMap.of("key", "2"));
        Service yellow = new Service(UUID.randomUUID(), UUID.randomUUID(), "storage", "beta", "/a/b/c", ImmutableMap.of("key", "3"));

        dynamicStore.put(redNodeId, ImmutableSet.of(red1, red2));
        dynamicStore.put(green.getNodeId(), ImmutableSet.of(green));
        dynamicStore.put(yellow.getNodeId(), ImmutableSet.of(yellow));

        assertEquals(resource.getServices("storage"), new Services("testing", ImmutableSet.of(red1, green, yellow)));
        assertEquals(resource.getServices("data"), new Services("testing", ImmutableSet.of(red2)));
        assertEquals(resource.getServices("unknown"), new Services("testing", Collections.<Service>emptySet()));
    }

    @Test
    public void testGetByTypeAndPool()
    {
        UUID redNodeId = UUID.randomUUID();
        Service red1 = new Service(UUID.randomUUID(), redNodeId, "storage", "alpha", "/a/b/c", ImmutableMap.of("key", "1"));
        Service red2 = new Service(UUID.randomUUID(), redNodeId, "data", "alpha", "/a/b/c", ImmutableMap.of("key", "4"));
        Service green = new Service(UUID.randomUUID(), UUID.randomUUID(), "storage", "alpha", "/a/b/c", ImmutableMap.of("key", "2"));
        Service yellow = new Service(UUID.randomUUID(), UUID.randomUUID(), "storage", "beta", "/a/b/c", ImmutableMap.of("key", "3"));

        dynamicStore.put(redNodeId, ImmutableSet.of(red1, red2));
        dynamicStore.put(green.getNodeId(), ImmutableSet.of(green));
        dynamicStore.put(yellow.getNodeId(), ImmutableSet.of(yellow));

        assertEquals(resource.getServices("storage", "alpha"), new Services("testing", ImmutableSet.of(red1, green)));
        assertEquals(resource.getServices("storage", "beta"), new Services("testing", ImmutableSet.of(yellow)));
        assertEquals(resource.getServices("storage", "unknown"), new Services("testing", Collections.<Service>emptySet()));
    }
}
