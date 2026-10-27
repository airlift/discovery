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
import io.airlift.testing.TestingClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.util.Set;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class TestStaticStore
{
    private static final Service BLUE = new Service(Id.random(), null, "storage", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableMap.of("http", "http://localhost:1111"));
    private static final Service RED = new Service(Id.random(), null, "storage", "poolB", "/US/West/SC4/rack1/host1/vm1/slot2", ImmutableMap.of("http", "http://localhost:2222"));
    private static final Service GREEN = new Service(Id.random(), null, "monitoring", "poolA", "/US/West/SC4/rack1/host1/vm1/slot3", ImmutableMap.of("http", "http://localhost:3333"));
    private static final Service YELLOW = new Service(Id.random(), null, "storage", "poolB", "/US/West/SC4/rack1/host1/vm1/slot3", ImmutableMap.of("http", "http://localhost:4444"));

    protected StaticStore store;
    protected TestingClock clock;

    protected abstract StaticStore initializeStore(Clock clock);

    @BeforeEach
    public void setup()
    {
        clock = new TestingClock();
        store = initializeStore(clock);
    }

    @Test
    public void testEmpty()
    {
        assertThat(store.getAll().isEmpty()).as("store should be empty").isTrue();
    }

    @Test
    public void testPutSingle()
    {
        store.put(BLUE);
        assertThat(store.getAll()).isEqualTo(ImmutableSet.of(BLUE));
    }

    @Test
    public void testPutMultiple()
    {
        store.put(BLUE);
        store.put(RED);
        store.put(GREEN);

        assertThat(store.getAll()).containsExactlyInAnyOrderElementsOf(ImmutableSet.of(BLUE, RED, GREEN));
    }

    @Test
    public void testGetByType()
    {
        store.put(BLUE);
        store.put(RED);
        store.put(GREEN);

        assertThat(store.get("storage")).containsExactlyInAnyOrderElementsOf(ImmutableSet.of(BLUE, RED));
        assertThat(store.get("monitoring")).containsExactlyInAnyOrderElementsOf(ImmutableSet.of(GREEN));
    }

    @Test
    public void testGetByTypeAndPool()
    {
        store.put(BLUE);
        store.put(RED);
        store.put(GREEN);
        store.put(YELLOW);

        assertThat(store.get("storage", "poolA")).containsExactlyInAnyOrderElementsOf(ImmutableSet.of(BLUE));
        assertThat(store.get("monitoring", "poolA")).containsExactlyInAnyOrderElementsOf(ImmutableSet.of(GREEN));
        assertThat(store.get("storage", "poolB")).containsExactlyInAnyOrderElementsOf(ImmutableSet.of(RED, YELLOW));
    }

    @Test
    public void testDelete()
    {
        store.put(BLUE);
        store.put(RED);
        store.put(GREEN);

        assertThat(store.getAll()).containsExactlyInAnyOrderElementsOf(ImmutableSet.of(BLUE, RED, GREEN));

        clock.increment(1, MILLISECONDS);

        store.delete(GREEN.getId());

        assertThat(store.getAll()).containsExactlyInAnyOrderElementsOf(ImmutableSet.of(BLUE, RED));
        assertThat(store.get("web").isEmpty()).isTrue();
        assertThat(store.get("web", "poolA").isEmpty()).isTrue();
    }

    @Test
    public void testCanHandleLotsOfServices()
    {
        ImmutableSet.Builder<Service> builder = ImmutableSet.builder();
        for (int i = 0; i < 5000; ++i) {
            builder.add(new Service(Id.random(), null, "storage", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableMap.of("http", "http://localhost:1111")));
        }
        Set<Service> services = builder.build();

        for (Service service : services) {
            store.put(service);
        }

        assertThat(store.getAll()).isEqualTo(services);
    }
}
