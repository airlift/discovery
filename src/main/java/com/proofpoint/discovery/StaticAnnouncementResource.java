package com.proofpoint.discovery;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;
import com.proofpoint.node.NodeInfo;

import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import java.net.URI;
import java.util.Set;
import java.util.UUID;

import static java.lang.String.format;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;

@Path("/v1/announcement/static")
public class StaticAnnouncementResource
{
    private final StaticStore store;
    private final NodeInfo nodeInfo;

    @Inject
    public StaticAnnouncementResource(StaticStore store, NodeInfo nodeInfo)
    {
        this.store = store;
        this.nodeInfo = nodeInfo;
    }

    @POST
    public Response post(StaticAnnouncement announcement, @Context UriInfo uriInfo)
    {
        if (!nodeInfo.getEnvironment().equals(announcement.getEnvironment())) {
            return Response.status(BAD_REQUEST)
                    .entity(format("Environment mismatch. Expected: %s, Provided: %s", nodeInfo.getEnvironment(), announcement.getEnvironment()))
                    .build();
        }

        UUID id = UUID.randomUUID();
        String location = Objects.firstNonNull(announcement.getLocation(), "/somewhere/" + id.toString());

        Service service = Service.copyOf(announcement)
                    .setId(UUID.randomUUID())
                    .setLocation(location)
                    .build();

        store.put(service);

        URI uri = UriBuilder.fromUri(uriInfo.getBaseUri()).path(StaticAnnouncementResource.class).path("{id}").build(id);
        return Response.created(uri).entity(service).build();
    }

    @GET
    public Services get()
    {
        return new Services(nodeInfo.getEnvironment(), store.getAll());
    }

    @DELETE
    @Path("{id}")
    public void delete(UUID id)
    {
        store.delete(id);
    }
}
