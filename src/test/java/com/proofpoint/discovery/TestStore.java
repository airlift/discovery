package com.proofpoint.discovery;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.UUID;

import static com.proofpoint.testing.Assertions.assertEqualsIgnoreOrder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public abstract class TestStore
{
    protected Store store;

    @Test
    public void testEmpty()
    {
        assertTrue(store.getAll().isEmpty(), "store should be empty");
    }

    @Test
    public void testPutSingle()
    {
        UUID nodeId = UUID.randomUUID();

        Service blue = new Service(UUID.randomUUID(), nodeId, "storage", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableMap.of("http", "http://localhost:1111"));

        store.put(nodeId, ImmutableSet.of(blue));

        assertEquals(store.getAll(), ImmutableSet.of(blue));
    }

    @Test
    public void testPutMultipleForSameNode()
    {
        UUID nodeId = UUID.randomUUID();

        Service blue = new Service(UUID.randomUUID(), nodeId, "storage", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableMap.of("http", "http://localhost:1111"));
        Service red = new Service(UUID.randomUUID(), nodeId, "web", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableMap.of("http", "http://localhost:2222"));
        Service green = new Service(UUID.randomUUID(), nodeId, "monitoring", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableMap.of("http", "http://localhost:3333"));

        store.put(nodeId, ImmutableSet.of(blue, red, green));

        assertEqualsIgnoreOrder(store.getAll(), ImmutableSet.of(blue, red, green));
    }

    @Test
    public void testReplace()
    {
        UUID nodeId = UUID.randomUUID();

        Service oldBlue = new Service(UUID.randomUUID(), nodeId, "storage", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableMap.of("http", "http://localhost:1111"));
        Service newBlue = new Service(UUID.randomUUID(), nodeId, "storage", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableMap.of("http", "http://localhost:2222"));

        assertTrue(store.put(nodeId, ImmutableSet.of(oldBlue)));
        assertFalse(store.put(nodeId, ImmutableSet.of(newBlue)));

        assertEquals(store.getAll(), ImmutableSet.of(newBlue));
    }

    @Test
    public void testPutMultipleForDifferentNodes()
    {
        Service blue = new Service(UUID.randomUUID(), UUID.randomUUID(), "storage", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableMap.of("http", "http://localhost:1111"));
        Service red = new Service(UUID.randomUUID(), UUID.randomUUID(), "web", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableMap.of("http", "http://localhost:2222"));
        Service green = new Service(UUID.randomUUID(), UUID.randomUUID(), "monitoring", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableMap.of("http", "http://localhost:3333"));

        store.put(blue.getNodeId(), ImmutableSet.of(blue));
        store.put(red.getNodeId(), ImmutableSet.of(red));
        store.put(green.getNodeId(), ImmutableSet.of(green));

        assertEqualsIgnoreOrder(store.getAll(), ImmutableSet.of(blue, red, green));
    }

    @Test
    public void testGetByType()
    {
        Service blue = new Service(UUID.randomUUID(), UUID.randomUUID(), "storage", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableMap.of("http", "http://localhost:1111"));
        Service red = new Service(UUID.randomUUID(), UUID.randomUUID(), "storage", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableMap.of("http", "http://localhost:2222"));
        Service green = new Service(UUID.randomUUID(), UUID.randomUUID(), "monitoring", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableMap.of("http", "http://localhost:3333"));

        store.put(blue.getNodeId(), ImmutableSet.of(blue));
        store.put(red.getNodeId(), ImmutableSet.of(red));
        store.put(green.getNodeId(), ImmutableSet.of(green));

        assertEqualsIgnoreOrder(store.get("storage"), ImmutableSet.of(blue, red));
        assertEqualsIgnoreOrder(store.get("monitoring"), ImmutableSet.of(green));
    }

    @Test
    public void testGetByTypeAndPool()
    {
        Service blue = new Service(UUID.randomUUID(), UUID.randomUUID(), "storage", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableMap.of("http", "http://localhost:1111"));
        Service red = new Service(UUID.randomUUID(), UUID.randomUUID(), "storage", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableMap.of("http", "http://localhost:2222"));
        Service green = new Service(UUID.randomUUID(), UUID.randomUUID(), "monitoring", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableMap.of("http", "http://localhost:3333"));
        Service yellow = new Service(UUID.randomUUID(), UUID.randomUUID(), "storage", "poolB", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableMap.of("http", "http://localhost:3333"));

        store.put(blue.getNodeId(), ImmutableSet.of(blue));
        store.put(red.getNodeId(), ImmutableSet.of(red));
        store.put(green.getNodeId(), ImmutableSet.of(green));
        store.put(yellow.getNodeId(), ImmutableSet.of(yellow));

        assertEqualsIgnoreOrder(store.get("storage", "poolA"), ImmutableSet.of(blue, red));
        assertEqualsIgnoreOrder(store.get("monitoring", "poolA"), ImmutableSet.of(green));
        assertEqualsIgnoreOrder(store.get("storage", "poolB"), ImmutableSet.of(yellow));
    }


    @Test
    public void testExpiration()
    {
        // TODO
    }

    @Test
    public void testDelete()
    {
        UUID blueNodeId = UUID.randomUUID();
        Service blue1 = new Service(UUID.randomUUID(), blueNodeId, "storage", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableMap.of("http", "http://localhost:1111"));
        Service blue2 = new Service(UUID.randomUUID(), blueNodeId, "web", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableMap.of("http", "http://localhost:2222"));
        Service red = new Service(UUID.randomUUID(), UUID.randomUUID(), "monitoring", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableMap.of("http", "http://localhost:3333"));

        store.put(blueNodeId, ImmutableSet.of(blue1, blue2));
        store.put(red.getNodeId(), ImmutableSet.of(red));

        assertEqualsIgnoreOrder(store.getAll(), ImmutableSet.of(blue1, blue2, red));

        store.delete(blueNodeId);

        assertEqualsIgnoreOrder(store.getAll(), ImmutableSet.of(red));
        assertTrue(store.get("storage").isEmpty());
        assertTrue(store.get("web", "poolA").isEmpty());
    }
}
