package com.proofpoint.discovery;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;
import com.proofpoint.node.NodeInfo;

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
import java.util.UUID;

import static java.lang.String.format;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;

@Path("/v1/announcement/{node_id}")
public class AnnouncementResource
{
    private final NodeInfo nodeInfo;
    private final Store store;

    @Inject
    public AnnouncementResource(Store store, NodeInfo nodeInfo)
    {
        this.store = store;
        this.nodeInfo = nodeInfo;
    }

    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    public Response put(@PathParam("node_id") UUID nodeId, @Context UriInfo uriInfo, Announcement announcement)
    {
        if (!nodeInfo.getEnvironment().equals(announcement.getEnvironment())) {
            return Response.status(BAD_REQUEST)
                    .entity(format("Environment mismatch. Expected: %s, Provided: %s", nodeInfo.getEnvironment(), announcement.getEnvironment()))
                    .build();
        }

        String location = Objects.firstNonNull(announcement.getLocation(), "/somewhere/" + nodeId.toString());

        ImmutableSet.Builder<Service> builder = new ImmutableSet.Builder<Service>();
        for (ServiceAnnouncement entry : announcement.getServices()) {
            Service descriptor = Service.copyOf(entry)
                    .setNodeId(nodeId)
                    .setLocation(location)
                    .build();
            builder.add(descriptor);
        }

        if (store.put(nodeId, builder.build())) {
            return Response.created(uriInfo.getRequestUri()).build();
        }

        return Response.noContent().build();
    }

    @DELETE
    public Response delete(@PathParam("node_id") UUID nodeId)
    {
        if (!store.delete(nodeId)) {
            return Response.status(NOT_FOUND).build();
        }

        return Response.noContent().build();
    }
}
