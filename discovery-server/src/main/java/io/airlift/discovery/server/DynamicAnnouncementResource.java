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

import com.google.common.base.Objects;
import io.airlift.node.NodeInfo;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import static java.lang.String.format;
import static javax.ws.rs.core.Response.Status.ACCEPTED;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;

@Path("/v1/announcement/{node_id}")
public class DynamicAnnouncementResource
{
    private final NodeInfo nodeInfo;
    private final DynamicStore dynamicStore;

    @Inject
    public DynamicAnnouncementResource(DynamicStore dynamicStore, NodeInfo nodeInfo)
    {
        this.dynamicStore = dynamicStore;
        this.nodeInfo = nodeInfo;
    }

    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    public Response put(@PathParam("node_id") Id<Node> nodeId, @Context UriInfo uriInfo, DynamicAnnouncement announcement)
    {
        if (!nodeInfo.getEnvironment().equals(announcement.getEnvironment())) {
            return Response.status(BAD_REQUEST)
                    .entity(format("Environment mismatch. Expected: %s, Provided: %s", nodeInfo.getEnvironment(), announcement.getEnvironment()))
                    .build();
        }

        String location = Objects.firstNonNull(announcement.getLocation(), "/somewhere/" + nodeId.toString());

        DynamicAnnouncement announcementWithLocation = DynamicAnnouncement.copyOf(announcement)
                .setLocation(location)
                .build();

        dynamicStore.put(nodeId, announcementWithLocation);

        return Response.status(ACCEPTED).build();
    }

    @DELETE
    public Response delete(@PathParam("node_id") Id<Node> nodeId)
    {
        dynamicStore.delete(nodeId);

        return Response.noContent().build();
    }

    @DELETE
    @Path("{app_type}")
    public Response delete(@PathParam("node_id") Id<Node> nodeId, @PathParam("app_type") String applicationType)
    {
        dynamicStore.delete(nodeId, applicationType);

        return Response.noContent().build();
    }
}
