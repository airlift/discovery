package com.proofpoint.discovery;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.proofpoint.units.Duration;
import org.joda.time.DateTime;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.inject.Provider;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Collections2.transform;
import static com.google.common.collect.Iterables.concat;
import static com.proofpoint.discovery.DynamicServiceAnnouncement.toServiceWith;
import static com.proofpoint.testing.Assertions.assertEqualsIgnoreOrder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public abstract class TestDynamicStore
{
    private static final Duration MAX_AGE = new Duration(1, TimeUnit.MINUTES);

    protected TestingTimeProvider currentTime;
    protected DynamicStore store;

    protected abstract DynamicStore initializeStore(DiscoveryConfig config, Provider<DateTime> timeProvider);

    @BeforeMethod
    public void setup()
    {
        currentTime = new TestingTimeProvider();
        DiscoveryConfig config = new DiscoveryConfig().setMaxAge(new Duration(1, TimeUnit.MINUTES));
        store = initializeStore(config, currentTime);
    }

    @Test
    public void testEmpty()
    {
        assertTrue(store.getAll().isEmpty(), "store should be empty");
    }

    @Test
    public void testPutSingle()
    {
        UUID nodeId = UUID.randomUUID();
        DynamicAnnouncement blue = new DynamicAnnouncement("testing", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableSet.of(
                new DynamicServiceAnnouncement(UUID.randomUUID(), "storage", "poolA", ImmutableMap.of("http", "http://localhost:1111"))
        ));

        assertTrue(store.put(nodeId, blue));

        assertEquals(store.getAll(), transform(blue.getServices(), toServiceWith(nodeId, blue.getLocation())));
    }

    @Test
    public void testExpires()
    {
        UUID nodeId = UUID.randomUUID();
        DynamicAnnouncement blue = new DynamicAnnouncement("testing", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableSet.of(
                new DynamicServiceAnnouncement(UUID.randomUUID(), "storage", "poolA", ImmutableMap.of("http", "http://localhost:1111"))
        ));

        assertTrue(store.put(nodeId, blue));
        advanceTimeBeyondMaxAge();
        assertEquals(store.getAll(), Collections.<Service>emptySet());
    }

    @Test
    public void testPutMultipleForSameNode()
    {
        UUID nodeId = UUID.randomUUID();
        DynamicAnnouncement announcement = new DynamicAnnouncement("testing", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableSet.of(
                new DynamicServiceAnnouncement(UUID.randomUUID(), "storage", "poolA", ImmutableMap.of("http", "http://localhost:1111")),
                new DynamicServiceAnnouncement(UUID.randomUUID(), "web", "poolA", ImmutableMap.of("http", "http://localhost:2222")),
                new DynamicServiceAnnouncement(UUID.randomUUID(), "monitoring", "poolA", ImmutableMap.of("http", "http://localhost:3333"))
        ));

        assertTrue(store.put(nodeId, announcement));

        assertEqualsIgnoreOrder(store.getAll(), transform(announcement.getServices(), toServiceWith(nodeId, announcement.getLocation())));
    }

    @Test
    public void testReplace()
    {
        UUID nodeId = UUID.randomUUID();

        DynamicAnnouncement oldAnnouncement = new DynamicAnnouncement("testing", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableSet.of(
                new DynamicServiceAnnouncement(UUID.randomUUID(), "storage", "poolA", ImmutableMap.of("http", "http://localhost:1111"))
        ));

        DynamicAnnouncement newAnnouncement = new DynamicAnnouncement("testing", "/US/West/SC4/rack1/host1/vm1/slot2", ImmutableSet.of(
                new DynamicServiceAnnouncement(UUID.randomUUID(), "storage", "poolA", ImmutableMap.of("http", "http://localhost:2222"))
        ));

        assertTrue(store.put(nodeId, oldAnnouncement));
        currentTime.increment();
        assertFalse(store.put(nodeId, newAnnouncement));

        assertEquals(store.getAll(), transform(newAnnouncement.getServices(), toServiceWith(nodeId, newAnnouncement.getLocation())));
    }

    @Test
    public void testReplaceExpired()
    {
        UUID nodeId = UUID.randomUUID();

        DynamicAnnouncement oldAnnouncement = new DynamicAnnouncement("testing", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableSet.of(
                new DynamicServiceAnnouncement(UUID.randomUUID(), "storage", "poolA", ImmutableMap.of("http", "http://localhost:1111"))
        ));

        DynamicAnnouncement newAnnouncement = new DynamicAnnouncement("testing", "/US/West/SC4/rack1/host1/vm1/slot2", ImmutableSet.of(
                new DynamicServiceAnnouncement(UUID.randomUUID(), "storage", "poolA", ImmutableMap.of("http", "http://localhost:2222"))
        ));

        assertTrue(store.put(nodeId, oldAnnouncement));
        advanceTimeBeyondMaxAge();
        assertTrue(store.put(nodeId, newAnnouncement));

        assertEqualsIgnoreOrder(store.getAll(), transform(newAnnouncement.getServices(), toServiceWith(nodeId, newAnnouncement.getLocation())));
    }

    @Test
    public void testPutMultipleForDifferentNodes()
    {
        UUID blueNodeId = UUID.randomUUID();
        DynamicAnnouncement blue = new DynamicAnnouncement("testing", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableSet.of(
                new DynamicServiceAnnouncement(UUID.randomUUID(), "storage", "poolA", ImmutableMap.of("http", "http://localhost:1111"))
        ));

        UUID redNodeId = UUID.randomUUID();
        DynamicAnnouncement red = new DynamicAnnouncement("testing", "/US/West/SC4/rack1/host1/vm1/slot2", ImmutableSet.of(
                new DynamicServiceAnnouncement(UUID.randomUUID(), "web", "poolA", ImmutableMap.of("http", "http://localhost:2222"))
        ));

        UUID greenNodeId = UUID.randomUUID();
        DynamicAnnouncement green = new DynamicAnnouncement("testing", "/US/West/SC4/rack1/host1/vm1/slot3", ImmutableSet.of(
                new DynamicServiceAnnouncement(UUID.randomUUID(), "monitoring", "poolA", ImmutableMap.of("http", "http://localhost:3333"))
        ));

        assertTrue(store.put(blueNodeId, blue));
        assertTrue(store.put(redNodeId, red));
        assertTrue(store.put(greenNodeId, green));

        assertEqualsIgnoreOrder(store.getAll(), concat(
                transform(blue.getServices(), toServiceWith(blueNodeId, blue.getLocation())),
                transform(red.getServices(), toServiceWith(redNodeId, red.getLocation())),
                transform(green.getServices(), toServiceWith(greenNodeId, green.getLocation()))));
    }

    @Test
    public void testGetByType()
    {
        UUID blueNodeId = UUID.randomUUID();
        DynamicAnnouncement blue = new DynamicAnnouncement("testing", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableSet.of(
                new DynamicServiceAnnouncement(UUID.randomUUID(), "storage", "poolA", ImmutableMap.of("http", "http://localhost:1111"))
        ));

        UUID redNodeId = UUID.randomUUID();
        DynamicAnnouncement red = new DynamicAnnouncement("testing", "/US/West/SC4/rack1/host1/vm1/slot2", ImmutableSet.of(
                new DynamicServiceAnnouncement(UUID.randomUUID(), "storage", "poolA", ImmutableMap.of("http", "http://localhost:2222"))
        ));

        UUID greenNodeId = UUID.randomUUID();
        DynamicAnnouncement green = new DynamicAnnouncement("testing", "/US/West/SC4/rack1/host1/vm1/slot3", ImmutableSet.of(
                new DynamicServiceAnnouncement(UUID.randomUUID(), "monitoring", "poolA", ImmutableMap.of("http", "http://localhost:3333"))
        ));

        assertTrue(store.put(blueNodeId, blue));
        assertTrue(store.put(redNodeId, red));
        assertTrue(store.put(greenNodeId, green));

        assertEqualsIgnoreOrder(store.get("storage"), concat(
                transform(blue.getServices(), toServiceWith(blueNodeId, blue.getLocation())),
                transform(red.getServices(), toServiceWith(redNodeId, red.getLocation()))));

        assertEqualsIgnoreOrder(store.get("monitoring"), transform(green.getServices(), toServiceWith(greenNodeId, green.getLocation())));
    }

    @Test
    public void testGetByTypeAndPool()
    {
        UUID blueNodeId = UUID.randomUUID();
        DynamicAnnouncement blue = new DynamicAnnouncement("testing", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableSet.of(
                new DynamicServiceAnnouncement(UUID.randomUUID(), "storage", "poolA", ImmutableMap.of("http", "http://localhost:1111"))
        ));

        UUID redNodeId = UUID.randomUUID();
        DynamicAnnouncement red = new DynamicAnnouncement("testing", "/US/West/SC4/rack1/host1/vm1/slot2", ImmutableSet.of(
                new DynamicServiceAnnouncement(UUID.randomUUID(), "storage", "poolA", ImmutableMap.of("http", "http://localhost:2222"))
        ));

        UUID greenNodeId = UUID.randomUUID();
        DynamicAnnouncement green = new DynamicAnnouncement("testing", "/US/West/SC4/rack1/host1/vm1/slot3", ImmutableSet.of(
                new DynamicServiceAnnouncement(UUID.randomUUID(), "monitoring", "poolA", ImmutableMap.of("http", "http://localhost:3333"))
        ));

        UUID yellowNodeId = UUID.randomUUID();
        DynamicAnnouncement yellow = new DynamicAnnouncement("testing", "/US/West/SC4/rack1/host1/vm1/slot4", ImmutableSet.of(
                new DynamicServiceAnnouncement(UUID.randomUUID(), "storage", "poolB", ImmutableMap.of("http", "http://localhost:4444"))
        ));

        assertTrue(store.put(blueNodeId, blue));
        assertTrue(store.put(redNodeId, red));
        assertTrue(store.put(greenNodeId, green));
        assertTrue(store.put(yellowNodeId, yellow));

        assertEqualsIgnoreOrder(store.get("storage", "poolA"), concat(
                transform(blue.getServices(), toServiceWith(blueNodeId, blue.getLocation())),
                transform(red.getServices(), toServiceWith(redNodeId, red.getLocation()))));

        assertEqualsIgnoreOrder(store.get("monitoring", "poolA"), concat(
                transform(green.getServices(), toServiceWith(greenNodeId, red.getLocation()))));

        assertEqualsIgnoreOrder(store.get("storage", "poolB"), concat(
                transform(yellow.getServices(), toServiceWith(yellowNodeId, red.getLocation()))));
    }

    @Test
    public void testDelete()
    {
        UUID blueNodeId = UUID.randomUUID();
        DynamicAnnouncement blue = new DynamicAnnouncement("testing", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableSet.of(
                new DynamicServiceAnnouncement(UUID.randomUUID(), "storage", "poolA", ImmutableMap.of("http", "http://localhost:1111")),
                new DynamicServiceAnnouncement(UUID.randomUUID(), "web", "poolA", ImmutableMap.of("http", "http://localhost:2222"))
        ));

        UUID redNodeId = UUID.randomUUID();
        DynamicAnnouncement red = new DynamicAnnouncement("testing", "/US/West/SC4/rack1/host1/vm1/slot2", ImmutableSet.of(
                new DynamicServiceAnnouncement(UUID.randomUUID(), "monitoring", "poolA", ImmutableMap.of("http", "http://localhost:2222"))
        ));

        assertTrue(store.put(blueNodeId, blue));
        assertTrue(store.put(redNodeId, red));

        assertEqualsIgnoreOrder(store.getAll(), concat(
                transform(blue.getServices(), toServiceWith(blueNodeId, blue.getLocation())),
                transform(red.getServices(), toServiceWith(redNodeId, red.getLocation()))));

        assertTrue(store.delete(blueNodeId));

        assertEqualsIgnoreOrder(store.getAll(), transform(red.getServices(), toServiceWith(redNodeId, red.getLocation())));

        assertTrue(store.get("storage").isEmpty());
        assertTrue(store.get("web", "poolA").isEmpty());
    }

    private void advanceTimeBeyondMaxAge()
    {
        currentTime.add(new Duration(MAX_AGE.toMillis() * 2, TimeUnit.MILLISECONDS));
    }
}
