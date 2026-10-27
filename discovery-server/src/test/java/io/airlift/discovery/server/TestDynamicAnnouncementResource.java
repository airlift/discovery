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
import io.airlift.discovery.store.RealTimeSupplier;
import io.airlift.jaxrs.testing.MockUriInfo;
import io.airlift.node.NodeInfo;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;

import static com.google.common.collect.Iterables.transform;
import static io.airlift.discovery.server.DynamicServiceAnnouncement.toServiceWith;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDynamicAnnouncementResource
{
    private InMemoryDynamicStore store;
    private DynamicAnnouncementResource resource;

    @BeforeEach
    public void setup()
    {
        store = new InMemoryDynamicStore(new DiscoveryConfig(), new RealTimeSupplier());
        resource = new DynamicAnnouncementResource(store, new NodeInfo("testing"));
    }

    @Test
    public void testPutNew()
    {
        DynamicAnnouncement announcement = new DynamicAnnouncement("testing", "alpha", "/a/b/c", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:1111")))
        );

        Id<Node> nodeId = Id.random();
        Response response = resource.put(nodeId, new MockUriInfo(URI.create("http://localhost:8080/v1/announcement/" + nodeId)), announcement);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(Response.Status.ACCEPTED.getStatusCode());

        assertThat(store.getAll()).containsExactlyInAnyOrderElementsOf(transform(announcement.getServiceAnnouncements(), toServiceWith(nodeId, announcement.getLocation(), announcement.getPool())));
    }

    @Test
    public void testReplace()
    {
        Id<Node> nodeId = Id.random();
        DynamicAnnouncement previous = new DynamicAnnouncement("testing", "alpha", "/a/b/c", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("key", "existing"))
        ));

        store.put(nodeId, previous);

        DynamicAnnouncement announcement = new DynamicAnnouncement("testing", "alpha", "/a/b/c", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("key", "new")))
        );

        Response response = resource.put(nodeId, new MockUriInfo(URI.create("http://localhost:8080/v1/announcement/" + nodeId)), announcement);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(Response.Status.ACCEPTED.getStatusCode());

        assertThat(store.getAll()).containsExactlyInAnyOrderElementsOf(transform(announcement.getServiceAnnouncements(), toServiceWith(nodeId, announcement.getLocation(), announcement.getPool())));
    }

    @Test
    public void testEnvironmentConflict()
    {
        DynamicAnnouncement announcement = new DynamicAnnouncement("production", "alpha", "/a/b/c", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:1111")))
        );

        Id<Node> nodeId = Id.random();
        Response response = resource.put(nodeId, new MockUriInfo(URI.create("http://localhost:8080/v1/announcement/" + nodeId)), announcement);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());

        assertThat(store.getAll().isEmpty()).isTrue();
    }

    @Test
    public void testDeleteExisting()
    {
        Id<Node> blueNodeId = Id.random();
        DynamicAnnouncement blue = new DynamicAnnouncement("testing", "alpha", "/a/b/c", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("key", "valueBlue"))
        ));

        Id<Node> redNodeId = Id.random();
        DynamicAnnouncement red = new DynamicAnnouncement("testing", "alpha", "/a/b/c", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("key", "valueBlue"))
        ));

        store.put(redNodeId, red);
        store.put(blueNodeId, blue);

        Response response = resource.delete(blueNodeId);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(Response.Status.NO_CONTENT.getStatusCode());

        assertThat(store.getAll()).containsExactlyInAnyOrderElementsOf(transform(red.getServiceAnnouncements(), toServiceWith(redNodeId, red.getLocation(), red.getPool())));
    }

    @Test
    public void testDeleteMissing()
    {
        Response response = resource.delete(Id.random());

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(Response.Status.NO_CONTENT.getStatusCode());

        assertThat(store.getAll().isEmpty()).isTrue();
    }

    @Test
    public void testMakesUpLocation()
    {
        DynamicAnnouncement announcement = new DynamicAnnouncement("testing", "alpha", null, ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:1111")))
        );

        Id<Node> nodeId = Id.valueOf("test123");
        Response response = resource.put(nodeId, new MockUriInfo(URI.create("http://localhost:8080/v1/announcement/" + nodeId)), announcement);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(Response.Status.ACCEPTED.getStatusCode());

        assertThat(store.getAll().size()).isEqualTo(1);
        Service service = store.getAll().iterator().next();
        assertThat(service.getId()).isEqualTo(service.getId());
        assertThat(service.getLocation()).isNotNull();
    }
}
