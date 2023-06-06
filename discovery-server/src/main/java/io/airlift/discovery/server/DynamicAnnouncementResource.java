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

import com.google.inject.Inject;
import io.airlift.node.NodeInfo;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;

import static com.google.common.base.MoreObjects.firstNonNull;
import static jakarta.ws.rs.core.Response.Status.ACCEPTED;
import static jakarta.ws.rs.core.Response.Status.BAD_REQUEST;
import static java.lang.String.format;

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

        String location = firstNonNull(announcement.getLocation(), "/somewhere/" + nodeId.toString());

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
}
