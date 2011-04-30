package com.proofpoint.discovery;

import com.google.inject.Inject;
import com.proofpoint.node.NodeInfo;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;


@Path("/v1/service")
public class ServiceResource
{
    private final Store store;
    private final NodeInfo node;

    @Inject
    public ServiceResource(Store store, NodeInfo node)
    {
        this.store = store;
        this.node = node;
    }

    @GET
    @Path("{type}/{pool}")
    @Produces(MediaType.APPLICATION_JSON)
    public Services getServices(@PathParam("type") String type, @PathParam("pool") String pool)
    {
        return new Services(node.getEnvironment(), store.get(type, pool));
    }

    @GET
    @Path("{type}")
    @Produces(MediaType.APPLICATION_JSON)
    public Services getServices(@PathParam("type") String type)
    {
        return new Services(node.getEnvironment(), store.get(type));
    }
}
