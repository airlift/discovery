package com.proofpoint.discovery;

import com.google.inject.Inject;
import com.proofpoint.node.NodeInfo;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import static com.google.common.collect.Sets.union;


@Path("/v1/service")
public class ServiceResource
{
    private final DynamicStore dynamicStore;
    private final StaticStore staticStore;
    private final NodeInfo node;

    @Inject
    public ServiceResource(DynamicStore dynamicStore, StaticStore staticStore, NodeInfo node)
    {
        this.dynamicStore = dynamicStore;
        this.staticStore = staticStore;
        this.node = node;
    }

    @GET
    @Path("{type}/{pool}")
    @Produces(MediaType.APPLICATION_JSON)
    public Services getServices(@PathParam("type") String type, @PathParam("pool") String pool)
    {
        return new Services(node.getEnvironment(), union(dynamicStore.get(type, pool), staticStore.get(type, pool)));
    }

    @GET
    @Path("{type}")
    @Produces(MediaType.APPLICATION_JSON)
    public Services getServices(@PathParam("type") String type)
    {
        return new Services(node.getEnvironment(), union(dynamicStore.get(type), staticStore.get(type)));
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Services getServices()
    {
        return new Services(node.getEnvironment(), union(dynamicStore.getAll(), staticStore.getAll()));
    }
}
