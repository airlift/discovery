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
import io.airlift.jaxrs.testing.MockUriInfo;
import io.airlift.node.NodeInfo;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;

public class TestStaticAnnouncementResource
{
    private InMemoryStaticStore store;
    private StaticAnnouncementResource resource;

    @BeforeEach
    public void setup()
    {
        store = new InMemoryStaticStore();
        resource = new StaticAnnouncementResource(store, new NodeInfo("testing"));
    }

    @Test
    public void testPost()
    {
        StaticAnnouncement announcement = new StaticAnnouncement("testing", "storage", "alpha", "/a/b", ImmutableMap.of("http", "http://localhost:1111"));

        Response response = resource.post(announcement, new MockUriInfo(URI.create("http://localhost:8080/v1/announcement/static")));

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());

        assertThat(store.getAll().size()).isEqualTo(1);
        Service service = store.getAll().iterator().next();

        assertThat(service.getId()).isNotNull();
        assertThat(service.getNodeId()).isNull();
        assertThat(service.getLocation()).isEqualTo(announcement.getLocation());
        assertThat(service.getType()).isEqualTo(announcement.getType());
        assertThat(service.getPool()).isEqualTo(announcement.getPool());
        assertThat(service.getProperties()).isEqualTo(announcement.getProperties());
    }

    @Test
    public void testEnvironmentConflict()
    {
        StaticAnnouncement announcement = new StaticAnnouncement("production", "storage", "alpha", "/a/b/c", ImmutableMap.of("http", "http://localhost:1111"));

        Response response = resource.post(announcement, new MockUriInfo(URI.create("http://localhost:8080/v1/announcement/static")));

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());

        assertThat(store.getAll().isEmpty()).isTrue();
    }

    @Test
    public void testDelete()
    {
        Service blue = new Service(Id.random(), null, "storage", "alpha", "/a/b/c", ImmutableMap.of("key", "valueBlue"));
        Service red = new Service(Id.random(), null, "storage", "alpha", "/a/b/c", ImmutableMap.of("key", "valueRed"));

        store.put(red);
        store.put(blue);

        resource.delete(blue.getId());
        assertThat(store.getAll()).isEqualTo(ImmutableSet.of(red));
    }

    @Test
    public void testMakesUpLocation()
    {
        StaticAnnouncement announcement = new StaticAnnouncement("testing", "storage", "alpha", null, ImmutableMap.of("http", "http://localhost:1111"));

        Response response = resource.post(announcement, new MockUriInfo(URI.create("http://localhost:8080/v1/announcement/")));

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());

        assertThat(store.getAll().size()).isEqualTo(1);
        Service service = store.getAll().iterator().next();
        assertThat(service.getId()).isEqualTo(service.getId());
        assertThat(service.getLocation()).isNotNull();
    }

    @Test
    public void testGet()
    {
        Service blue = new Service(Id.random(), null, "storage", "alpha", "/a/b/c", ImmutableMap.of("key", "valueBlue"));
        Service red = new Service(Id.random(), null, "storage", "alpha", "/a/b/c", ImmutableMap.of("key", "valueRed"));

        store.put(red);
        store.put(blue);

        Services actual = resource.get();
        Services expected = new Services("testing", ImmutableSet.of(red, blue));

        assertThat(actual).isEqualTo(expected);
    }
}
